%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_reader).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([start_link/0, info_keys/0, info/1, info/2, shutdown/2]).

-export([system_continue/3, system_terminate/4, system_code_change/4]).

-export([init/1, mainloop/3]).

-export([server_properties/0]).

-export([analyze_frame/3]).

-export([emit_stats/1]).

-import(gen_tcp).
-import(fprof).
-import(inet).
-import(prim_inet).

-define(HANDSHAKE_TIMEOUT, 10).
-define(NORMAL_TIMEOUT, 3).
-define(CLOSING_TIMEOUT, 1).
-define(CHANNEL_TERMINATION_TIMEOUT, 3).
-define(SILENT_CLOSE_DELAY, 3).
-define(FRAME_MAX, 131072). %% set to zero once QPid fix their negotiation

%---------------------------------------------------------------------------

-record(v1, {sock, connection, callback, recv_ref, connection_state,
             queue_collector, stats_timer}).

-define(STATISTICS_KEYS, [pid, recv_oct, recv_cnt, send_oct, send_cnt,
                          send_pend, state, channels]).

-define(CREATION_EVENT_KEYS, [pid, address, port, peer_address, peer_port,
                              protocol, user, vhost, timeout, frame_max,
                              client_properties]).

-define(INFO_KEYS, ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [pid]).

%% connection lifecycle
%%
%% all state transitions and terminations are marked with *...*
%%
%% The lifecycle begins with: start handshake_timeout timer, *pre-init*
%%
%% all states, unless specified otherwise:
%%   socket error -> *exit*
%%   socket close -> *throw*
%%   writer send failure -> *throw*
%%   forced termination -> *exit*
%%   handshake_timeout -> *throw*
%% pre-init:
%%   receive protocol header -> send connection.start, *starting*
%% starting:
%%   receive connection.start_ok -> send connection.tune, *tuning*
%% tuning:
%%   receive connection.tune_ok -> start heartbeats, *opening*
%% opening:
%%   receive connection.open -> send connection.open_ok, *running*
%% running:
%%   receive connection.close ->
%%     tell channels to terminate gracefully
%%     if no channels then send connection.close_ok, start
%%        terminate_connection timer, *closed*
%%     else *closing*
%%   forced termination
%%   -> wait for channels to terminate forcefully, start
%%      terminate_connection timer, send close, *exit*
%%   channel exit with hard error
%%   -> log error, wait for channels to terminate forcefully, start
%%      terminate_connection timer, send close, *closed*
%%   channel exit with soft error
%%   -> log error, mark channel as closing, *running*
%%   handshake_timeout -> ignore, *running*
%%   heartbeat timeout -> *throw*
%% closing:
%%   socket close -> *terminate*
%%   receive connection.close -> send connection.close_ok,
%%     *closing*
%%   receive frame -> ignore, *closing*
%%   handshake_timeout -> ignore, *closing*
%%   heartbeat timeout -> *throw*
%%   channel exit with hard error
%%   -> log error, wait for channels to terminate forcefully, start
%%      terminate_connection timer, send close, *closed*
%%   channel exit with soft error
%%   -> log error, mark channel as closing
%%      if last channel to exit then send connection.close_ok,
%%         start terminate_connection timer, *closed*
%%      else *closing*
%%   channel exits normally
%%   -> if last channel to exit then send connection.close_ok,
%%      start terminate_connection timer, *closed*
%% closed:
%%   socket close -> *terminate*
%%   receive connection.close -> send connection.close_ok,
%%     *closed*
%%   receive connection.close_ok -> self() ! terminate_connection,
%%     *closed*
%%   receive frame -> ignore, *closed*
%%   terminate_connection timeout -> *terminate*
%%   handshake_timeout -> ignore, *closed*
%%   heartbeat timeout -> *throw*
%%   channel exit -> log error, *closed*
%%
%%
%% TODO: refactor the code so that the above is obvious

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(info_keys/0 :: () -> [rabbit_types:info_key()]).
-spec(info/1 :: (pid()) -> [rabbit_types:info()]).
-spec(info/2 :: (pid(), [rabbit_types:info_key()]) -> [rabbit_types:info()]).
-spec(emit_stats/1 :: (pid()) -> 'ok').
-spec(shutdown/2 :: (pid(), string()) -> 'ok').
-spec(server_properties/0 :: () -> rabbit_framing:amqp_table()).

-endif.

%%--------------------------------------------------------------------------

start_link() ->
    {ok, proc_lib:spawn_link(?MODULE, init, [self()])}.

shutdown(Pid, Explanation) ->
    gen_server:call(Pid, {shutdown, Explanation}, infinity).

init(Parent) ->
    Deb = sys:debug_options([]),
    receive
        {go, Sock, SockTransform} ->
            start_connection(Parent, Deb, Sock, SockTransform)
    end.

system_continue(Parent, Deb, State) ->
    ?MODULE:mainloop(Parent, Deb, State).

system_terminate(Reason, _Parent, _Deb, _State) ->
    exit(Reason).

system_code_change(Misc, _Module, _OldVsn, _Extra) ->
    {ok, Misc}.

info_keys() -> ?INFO_KEYS.

info(Pid) ->
    gen_server:call(Pid, info, infinity).

info(Pid, Items) ->
    case gen_server:call(Pid, {info, Items}, infinity) of
        {ok, Res}      -> Res;
        {error, Error} -> throw(Error)
    end.

emit_stats(Pid) ->
    gen_server:cast(Pid, emit_stats).

setup_profiling() ->
    Value = rabbit_misc:get_config(profiling_enabled, false),
    case Value of
        once ->
            rabbit_log:info("Enabling profiling for this connection, "
                            "and disabling for subsequent.~n"),
            rabbit_misc:set_config(profiling_enabled, false),
            fprof:trace(start);
        true ->
            rabbit_log:info("Enabling profiling for this connection.~n"),
            fprof:trace(start);
        false ->
            ok
    end,
    Value.

teardown_profiling(Value) ->
    case Value of
        false ->
            ok;
        _ ->
            rabbit_log:info("Completing profiling for this connection.~n"),
            fprof:trace(stop),
            fprof:profile(),
            fprof:analyse([{dest, []}, {cols, 100}])
    end.

server_properties() ->
    {ok, Product} = application:get_key(rabbit, id),
    {ok, Version} = application:get_key(rabbit, vsn),
    [{list_to_binary(K), longstr, list_to_binary(V)} ||
        {K, V} <- [{"product",     Product},
                   {"version",     Version},
                   {"platform",    "Erlang/OTP"},
                   {"copyright",   ?COPYRIGHT_MESSAGE},
                   {"information", ?INFORMATION_MESSAGE}]].

inet_op(F) -> rabbit_misc:throw_on_error(inet_error, F).

socket_op(Sock, Fun) ->
    case Fun(Sock) of
        {ok, Res}       -> Res;
        {error, Reason} -> rabbit_log:error("error on TCP connection ~p:~p~n",
                                            [self(), Reason]),
                           rabbit_log:info("closing TCP connection ~p~n",
                                           [self()]),
                           exit(normal)
    end.

start_connection(Parent, Deb, Sock, SockTransform) ->
    process_flag(trap_exit, true),
    {PeerAddress, PeerPort} = socket_op(Sock, fun rabbit_net:peername/1),
    PeerAddressS = inet_parse:ntoa(PeerAddress),
    rabbit_log:info("starting TCP connection ~p from ~s:~p~n",
                    [self(), PeerAddressS, PeerPort]),
    ClientSock = socket_op(Sock, SockTransform),
    erlang:send_after(?HANDSHAKE_TIMEOUT * 1000, self(),
                      handshake_timeout),
    ProfilingValue = setup_profiling(),
    {ok, Collector} = rabbit_queue_collector:start_link(),
    try
        mainloop(Parent, Deb, switch_callback(
                                #v1{sock = ClientSock,
                                    connection = #connection{
                                      user = none,
                                      timeout_sec = ?HANDSHAKE_TIMEOUT,
                                      frame_max = ?FRAME_MIN_SIZE,
                                      vhost = none,
                                      client_properties = none,
                                      protocol = none},
                                    callback = uninitialized_callback,
                                    recv_ref = none,
                                    connection_state = pre_init,
                                    queue_collector = Collector,
                                    stats_timer =
                                        rabbit_event:init_stats_timer()},
                                handshake, 8))
    catch
        Ex -> (if Ex == connection_closed_abruptly ->
                       fun rabbit_log:warning/2;
                  true ->
                       fun rabbit_log:error/2
               end)("exception on TCP connection ~p from ~s:~p~n~p~n",
                    [self(), PeerAddressS, PeerPort, Ex])
    after
        rabbit_log:info("closing TCP connection ~p from ~s:~p~n",
                        [self(), PeerAddressS, PeerPort]),
        %% We don't close the socket explicitly. The reader is the
        %% controlling process and hence its termination will close
        %% the socket. Furthermore, gen_tcp:close/1 waits for pending
        %% output to be sent, which results in unnecessary delays.
        %%
        %% gen_tcp:close(ClientSock),
        teardown_profiling(ProfilingValue),
        rabbit_queue_collector:shutdown(Collector),
        rabbit_misc:unlink_and_capture_exit(Collector),
        rabbit_event:notify(connection_closed, [{pid, self()}])
    end,
    done.

mainloop(Parent, Deb, State = #v1{sock= Sock, recv_ref = Ref}) ->
    %%?LOGDEBUG("Reader mainloop: ~p bytes available, need ~p~n", [HaveBytes, WaitUntilNBytes]),
    receive
        {inet_async, Sock, Ref, {ok, Data}} ->
            {State1, Callback1, Length1} =
                handle_input(State#v1.callback, Data,
                             State#v1{recv_ref = none}),
            mainloop(Parent, Deb,
                     switch_callback(State1, Callback1, Length1));
        {inet_async, Sock, Ref, {error, closed}} ->
            if State#v1.connection_state =:= closed ->
                    State;
               true ->
                    throw(connection_closed_abruptly)
            end;
        {inet_async, Sock, Ref, {error, Reason}} ->
            throw({inet_error, Reason});
        {'EXIT', Parent, Reason} ->
            terminate(io_lib:format("broker forced connection closure "
                                    "with reason '~w'", [Reason]), State),
            %% this is what we are expected to do according to
            %% http://www.erlang.org/doc/man/sys.html
            %%
            %% If we wanted to be *really* nice we should wait for a
            %% while for clients to close the socket at their end,
            %% just as we do in the ordinary error case. However,
            %% since this termination is initiated by our parent it is
            %% probably more important to exit quickly.
            exit(Reason);
        {channel_exit, _Chan, E = {writer, send_failed, _Error}} ->
            throw(E);
        {channel_exit, Channel, Reason} ->
            mainloop(Parent, Deb, handle_channel_exit(Channel, Reason, State));
        {'EXIT', Pid, Reason} ->
            mainloop(Parent, Deb, handle_dependent_exit(Pid, Reason, State));
        terminate_connection ->
            State;
        handshake_timeout ->
            if State#v1.connection_state =:= running orelse
               State#v1.connection_state =:= closing orelse
               State#v1.connection_state =:= closed ->
                    mainloop(Parent, Deb, State);
               true ->
                    throw({handshake_timeout, State#v1.callback})
            end;
        timeout ->
            throw({timeout, State#v1.connection_state});
        {'$gen_call', From, {shutdown, Explanation}} ->
            {ForceTermination, NewState} = terminate(Explanation, State),
            gen_server:reply(From, ok),
            case ForceTermination of
                force  -> ok;
                normal -> mainloop(Parent, Deb, NewState)
            end;
        {'$gen_call', From, info} ->
            gen_server:reply(From, infos(?INFO_KEYS, State)),
            mainloop(Parent, Deb, State);
        {'$gen_call', From, {info, Items}} ->
            gen_server:reply(From, try {ok, infos(Items, State)}
                                   catch Error -> {error, Error}
                                   end),
            mainloop(Parent, Deb, State);
        {'$gen_cast', emit_stats} ->
            internal_emit_stats(State),
            mainloop(Parent, Deb,
                     State#v1{stats_timer =
                                  rabbit_event:reset_stats_timer_after(
                                    State#v1.stats_timer)});
        {system, From, Request} ->
            sys:handle_system_msg(Request, From,
                                  Parent, ?MODULE, Deb, State);
        Other ->
            %% internal error -> something worth dying for
            exit({unexpected_message, Other})
    end.

switch_callback(OldState, NewCallback, Length) ->
    Ref = inet_op(fun () -> rabbit_net:async_recv(
                              OldState#v1.sock, Length, infinity) end),
    OldState#v1{callback = NewCallback,
                recv_ref = Ref}.

terminate(Explanation, State = #v1{connection_state = running}) ->
    {normal, send_exception(State, 0,
                            rabbit_misc:amqp_error(
                              connection_forced, Explanation, [], none))};
terminate(_Explanation, State) ->
    {force, State}.

close_connection(State = #v1{connection = #connection{
                               timeout_sec = TimeoutSec}}) ->
    %% We terminate the connection after the specified interval, but
    %% no later than ?CLOSING_TIMEOUT seconds.
    TimeoutMillisec =
        1000 * if TimeoutSec > 0 andalso
                  TimeoutSec < ?CLOSING_TIMEOUT -> TimeoutSec;
                  true -> ?CLOSING_TIMEOUT
               end,
    erlang:send_after(TimeoutMillisec, self(), terminate_connection),
    State#v1{connection_state = closed}.

close_channel(Channel, State) ->
    put({channel, Channel}, closing),
    State.

handle_channel_exit(Channel, Reason, State) ->
    handle_exception(State, Channel, Reason).

handle_dependent_exit(Pid, normal, State) ->
    erase({chpid, Pid}),
    maybe_close(State);
handle_dependent_exit(Pid, Reason, State) ->
    case channel_cleanup(Pid) of
        undefined -> exit({abnormal_dependent_exit, Pid, Reason});
        Channel   -> maybe_close(handle_exception(State, Channel, Reason))
    end.

channel_cleanup(Pid) ->
    case get({chpid, Pid}) of
        undefined          -> undefined;
        {channel, Channel} -> erase({channel, Channel}),
                              erase({chpid, Pid}),
                              Channel
    end.

all_channels() -> [Pid || {{chpid, Pid},_} <- get()].

terminate_channels() ->
    NChannels = length([exit(Pid, normal) || Pid <- all_channels()]),
    if NChannels > 0 ->
            Timeout = 1000 * ?CHANNEL_TERMINATION_TIMEOUT * NChannels,
            TimerRef = erlang:send_after(Timeout, self(), cancel_wait),
            wait_for_channel_termination(NChannels, TimerRef);
       true -> ok
    end.

wait_for_channel_termination(0, TimerRef) ->
    case erlang:cancel_timer(TimerRef) of
        false -> receive
                     cancel_wait -> ok
                 end;
        _     -> ok
    end;

wait_for_channel_termination(N, TimerRef) ->
    receive
        {'EXIT', Pid, Reason} ->
            case channel_cleanup(Pid) of
                undefined ->
                    exit({abnormal_dependent_exit, Pid, Reason});
                Channel ->
                    case Reason of
                        normal -> ok;
                        _ ->
                            rabbit_log:error(
                              "connection ~p, channel ~p - "
                              "error while terminating:~n~p~n",
                              [self(), Channel, Reason])
                    end,
                    wait_for_channel_termination(N-1, TimerRef)
            end;
        cancel_wait ->
            exit(channel_termination_timeout)
    end.

maybe_close(State = #v1{connection_state = closing,
                        queue_collector = Collector,
                        connection = #connection{protocol = Protocol},
                        sock = Sock}) ->
    case all_channels() of
        [] ->
            %% Spec says "Exclusive queues may only be accessed by the current
            %% connection, and are deleted when that connection closes."
            %% This does not strictly imply synchrony, but in practice it seems
            %% to be what people assume.
            rabbit_queue_collector:delete_all(Collector),
            ok = send_on_channel0(Sock, #'connection.close_ok'{}, Protocol),
            close_connection(State);
        _  -> State
    end;
maybe_close(State) ->
    State.

handle_frame(Type, 0, Payload,
             State = #v1{connection_state = CS,
                         connection = #connection{protocol = Protocol}})
  when CS =:= closing; CS =:= closed ->
    case analyze_frame(Type, Payload, Protocol) of
        {method, MethodName, FieldsBin} ->
            handle_method0(MethodName, FieldsBin, State);
        _Other -> State
    end;
handle_frame(_Type, _Channel, _Payload, State = #v1{connection_state = CS})
  when CS =:= closing; CS =:= closed ->
    State;
handle_frame(Type, 0, Payload,
             State = #v1{connection = #connection{protocol = Protocol}}) ->
    case analyze_frame(Type, Payload, Protocol) of
        error     -> throw({unknown_frame, 0, Type, Payload});
        heartbeat -> State;
        {method, MethodName, FieldsBin} ->
            handle_method0(MethodName, FieldsBin, State);
        Other -> throw({unexpected_frame_on_channel0, Other})
    end;
handle_frame(Type, Channel, Payload,
             State = #v1{connection = #connection{protocol = Protocol}}) ->
    case analyze_frame(Type, Payload, Protocol) of
        error         -> throw({unknown_frame, Channel, Type, Payload});
        heartbeat     -> throw({unexpected_heartbeat_frame, Channel});
        AnalyzedFrame ->
            %%?LOGDEBUG("Ch ~p Frame ~p~n", [Channel, AnalyzedFrame]),
            case get({channel, Channel}) of
                {chpid, ChPid} ->
                    case AnalyzedFrame of
                        {method, 'channel.close', _} ->
                            erase({channel, Channel});
                        _ -> ok
                    end,
                    ok = rabbit_framing_channel:process(ChPid, AnalyzedFrame),
                    State;
                closing ->
                    %% According to the spec, after sending a
                    %% channel.close we must ignore all frames except
                    %% channel.close and channel.close_ok.  In the
                    %% event of a channel.close, we should send back a
                    %% channel.close_ok.
                    case AnalyzedFrame of
                        {method, 'channel.close_ok', _} ->
                            erase({channel, Channel});
                        {method, 'channel.close', _} ->
                            %% We're already closing this channel, so
                            %% there's no cleanup to do (notify
                            %% queues, etc.)
                            ok = rabbit_writer:send_command(State#v1.sock,
                                                            #'channel.close_ok'{});
                        _ -> ok
                    end,
                    State;
                undefined ->
                    case State#v1.connection_state of
                        running -> ok = send_to_new_channel(
                                          Channel, AnalyzedFrame, State),
                                   State;
                        Other   -> throw({channel_frame_while_starting,
                                          Channel, Other, AnalyzedFrame})
                    end
            end
    end.

analyze_frame(?FRAME_METHOD,
              <<ClassId:16, MethodId:16, MethodFields/binary>>,
              Protocol) ->
    MethodName = Protocol:lookup_method_name({ClassId, MethodId}),
    {method, MethodName, MethodFields};
analyze_frame(?FRAME_HEADER,
              <<ClassId:16, Weight:16, BodySize:64, Properties/binary>>,
              _Protocol) ->
    {content_header, ClassId, Weight, BodySize, Properties};
analyze_frame(?FRAME_BODY, Body, _Protocol) ->
    {content_body, Body};
analyze_frame(?FRAME_HEARTBEAT, <<>>, _Protocol) ->
    heartbeat;
analyze_frame(_Type, _Body, _Protocol) ->
    error.

handle_input(frame_header, <<Type:8,Channel:16,PayloadSize:32>>, State) ->
    %%?LOGDEBUG("Got frame header: ~p/~p/~p~n", [Type, Channel, PayloadSize]),
    {ensure_stats_timer(State), {frame_payload, Type, Channel, PayloadSize},
     PayloadSize + 1};

handle_input({frame_payload, Type, Channel, PayloadSize}, PayloadAndMarker, State) ->
    case PayloadAndMarker of
        <<Payload:PayloadSize/binary, ?FRAME_END>> ->
            %%?LOGDEBUG("Frame completed: ~p/~p/~p~n", [Type, Channel, Payload]),
            NewState = handle_frame(Type, Channel, Payload, State),
            {NewState, frame_header, 7};
        _ ->
            throw({bad_payload, PayloadAndMarker})
    end;

%% The two rules pertaining to version negotiation:
%%
%% * If the server cannot support the protocol specified in the
%% protocol header, it MUST respond with a valid protocol header and
%% then close the socket connection.
%%
%% * The server MUST provide a protocol version that is lower than or
%% equal to that requested by the client in the protocol header.
handle_input(handshake, <<"AMQP", 0, 0, 9, 1>>, State) ->
    start_connection({0, 9, 1}, rabbit_framing_amqp_0_9_1, State);

%% This is the protocol header for 0-9, which we can safely treat as
%% though it were 0-9-1.
handle_input(handshake, <<"AMQP", 1, 1, 0, 9>>, State) ->
    start_connection({0, 9, 0}, rabbit_framing_amqp_0_9_1, State);

%% This is what most clients send for 0-8.  The 0-8 spec, confusingly,
%% defines the version as 8-0.
handle_input(handshake, <<"AMQP", 1, 1, 8, 0>>, State) ->
    start_connection({8, 0, 0}, rabbit_framing_amqp_0_8, State);

%% The 0-8 spec as on the AMQP web site actually has this as the
%% protocol header; some libraries e.g., py-amqplib, send it when they
%% want 0-8.
handle_input(handshake, <<"AMQP", 1, 1, 9, 1>>, State) ->
    start_connection({8, 0, 0}, rabbit_framing_amqp_0_8, State);

handle_input(handshake, <<"AMQP", A, B, C, D>>, #v1{sock = Sock}) ->
    refuse_connection(Sock, {bad_version, A, B, C, D});

handle_input(handshake, Other, #v1{sock = Sock}) ->
    refuse_connection(Sock, {bad_header, Other});

handle_input(Callback, Data, _State) ->
    throw({bad_input, Callback, Data}).

%% Offer a protocol version to the client.  Connection.start only
%% includes a major and minor version number, Luckily 0-9 and 0-9-1
%% are similar enough that clients will be happy with either.
start_connection({ProtocolMajor, ProtocolMinor, _ProtocolRevision},
                 Protocol,
                 State = #v1{sock = Sock, connection = Connection}) ->
    Start = #'connection.start'{ version_major = ProtocolMajor,
                                 version_minor = ProtocolMinor,
                                 server_properties = server_properties(),
                                 mechanisms = <<"PLAIN AMQPLAIN">>,
                                 locales = <<"en_US">> },
    ok = send_on_channel0(Sock, Start, Protocol),
    {State#v1{connection = Connection#connection{
                             timeout_sec = ?NORMAL_TIMEOUT,
                             protocol = Protocol},
              connection_state = starting},
     frame_header, 7}.

refuse_connection(Sock, Exception) ->
    ok = inet_op(fun () -> rabbit_net:send(Sock, <<"AMQP",0,0,9,1>>) end),
    throw(Exception).

ensure_stats_timer(State = #v1{stats_timer = StatsTimer}) ->
    Self = self(),
    State#v1{stats_timer = rabbit_event:ensure_stats_timer_after(
                             StatsTimer,
                             fun() -> emit_stats(Self) end)}.

%%--------------------------------------------------------------------------

handle_method0(MethodName, FieldsBin,
               State = #v1{connection = #connection{protocol = Protocol}}) ->
    try
        handle_method0(Protocol:decode_method_fields(MethodName, FieldsBin),
                       State)
    catch exit:Reason ->
            CompleteReason = case Reason of
                                 #amqp_error{method = none} ->
                                     Reason#amqp_error{method = MethodName};
                                 OtherReason -> OtherReason
                             end,
            case State#v1.connection_state of
                running -> send_exception(State, 0, CompleteReason);
                %% We don't trust the client at this point - force
                %% them to wait for a bit so they can't DOS us with
                %% repeated failed logins etc.
                Other   -> timer:sleep(?SILENT_CLOSE_DELAY * 1000),
                           throw({channel0_error, Other, CompleteReason})
            end
    end.

handle_method0(#'connection.start_ok'{mechanism = Mechanism,
                                      response = Response,
                                      client_properties = ClientProperties},
               State = #v1{connection_state = starting,
                           connection = Connection =
                               #connection{protocol = Protocol},
                           sock = Sock}) ->
    User = rabbit_access_control:check_login(Mechanism, Response),
    Tune = #'connection.tune'{channel_max = 0,
                              frame_max = ?FRAME_MAX,
                              heartbeat = 0},
    ok = send_on_channel0(Sock, Tune, Protocol),
    State#v1{connection_state = tuning,
             connection = Connection#connection{
                            user = User,
                            client_properties = ClientProperties}};
handle_method0(#'connection.tune_ok'{frame_max = FrameMax,
                                     heartbeat = ClientHeartbeat},
               State = #v1{connection_state = tuning,
                           connection = Connection,
                           sock = Sock}) ->
    if (FrameMax /= 0) and (FrameMax < ?FRAME_MIN_SIZE) ->
            rabbit_misc:protocol_error(
              not_allowed, "frame_max=~w < ~w min size",
              [FrameMax, ?FRAME_MIN_SIZE]);
       (?FRAME_MAX /= 0) and (FrameMax > ?FRAME_MAX) ->
            rabbit_misc:protocol_error(
              not_allowed, "frame_max=~w > ~w max size",
              [FrameMax, ?FRAME_MAX]);
       true ->
            rabbit_heartbeat:start_heartbeat(Sock, ClientHeartbeat),
            State#v1{connection_state = opening,
                     connection = Connection#connection{
                                    timeout_sec = ClientHeartbeat,
                                    frame_max = FrameMax}}
    end;

handle_method0(#'connection.open'{virtual_host = VHostPath},

               State = #v1{connection_state = opening,
                           connection = Connection = #connection{
                                          user = User,
                                          protocol = Protocol},
                           sock = Sock}) ->
    ok = rabbit_access_control:check_vhost_access(User, VHostPath),
    NewConnection = Connection#connection{vhost = VHostPath},
    ok = send_on_channel0(Sock, #'connection.open_ok'{}, Protocol),
    State1 = State#v1{connection_state = running,
                      connection = NewConnection},
    rabbit_event:notify(
      connection_created,
      [{Item, i(Item, State1)} || Item <- ?CREATION_EVENT_KEYS]),
    State1;
handle_method0(#'connection.close'{},
               State = #v1{connection_state = running}) ->
    lists:foreach(fun rabbit_framing_channel:shutdown/1, all_channels()),
    maybe_close(State#v1{connection_state = closing});
handle_method0(#'connection.close'{},
               State = #v1{connection_state = CS,
                           connection = #connection{protocol = Protocol},
                           sock = Sock})
  when CS =:= closing; CS =:= closed ->
    %% We're already closed or closing, so we don't need to cleanup
    %% anything.
    ok = send_on_channel0(Sock, #'connection.close_ok'{}, Protocol),
    State;
handle_method0(#'connection.close_ok'{},
               State = #v1{connection_state = closed}) ->
    self() ! terminate_connection,
    State;
handle_method0(_Method, State = #v1{connection_state = CS})
  when CS =:= closing; CS =:= closed ->
    State;
handle_method0(_Method, #v1{connection_state = S}) ->
    rabbit_misc:protocol_error(
      channel_error, "unexpected method in connection state ~w", [S]).

send_on_channel0(Sock, Method, Protocol) ->
    ok = rabbit_writer:internal_send_command(Sock, 0, Method, Protocol).

%%--------------------------------------------------------------------------

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(pid, #v1{}) ->
    self();
i(address, #v1{sock = Sock}) ->
    {ok, {A, _}} = rabbit_net:sockname(Sock),
    A;
i(port, #v1{sock = Sock}) ->
    {ok, {_, P}} = rabbit_net:sockname(Sock),
    P;
i(peer_address, #v1{sock = Sock}) ->
    {ok, {A, _}} = rabbit_net:peername(Sock),
    A;
i(peer_port, #v1{sock = Sock}) ->
    {ok, {_, P}} = rabbit_net:peername(Sock),
    P;
i(SockStat, #v1{sock = Sock}) when SockStat =:= recv_oct;
                                   SockStat =:= recv_cnt;
                                   SockStat =:= send_oct;
                                   SockStat =:= send_cnt;
                                   SockStat =:= send_pend ->
    case rabbit_net:getstat(Sock, [SockStat]) of
        {ok, [{SockStat, StatVal}]} -> StatVal;
        {error, einval}             -> undefined;
        {error, Error}              -> throw({cannot_get_socket_stats, Error})
    end;
i(state, #v1{connection_state = S}) ->
    S;
i(channels, #v1{}) ->
    length(all_channels());
i(protocol, #v1{connection = #connection{protocol = none}}) ->
    none;
i(protocol, #v1{connection = #connection{protocol = Protocol}}) ->
    Protocol:version();
i(user, #v1{connection = #connection{user = #user{username = Username}}}) ->
    Username;
i(user, #v1{connection = #connection{user = none}}) ->
    '';
i(vhost, #v1{connection = #connection{vhost = VHost}}) ->
    VHost;
i(timeout, #v1{connection = #connection{timeout_sec = Timeout}}) ->
    Timeout;
i(frame_max, #v1{connection = #connection{frame_max = FrameMax}}) ->
    FrameMax;
i(client_properties, #v1{connection = #connection{
                           client_properties = ClientProperties}}) ->
    ClientProperties;
i(Item, #v1{}) ->
    throw({bad_argument, Item}).

%%--------------------------------------------------------------------------

send_to_new_channel(Channel, AnalyzedFrame,
                    State = #v1{queue_collector = Collector}) ->
    #v1{sock = Sock, connection = #connection{
                       frame_max = FrameMax,
                       user = #user{username = Username},
                       vhost = VHost,
                       protocol = Protocol}} = State,
    {ok, WriterPid} = rabbit_writer:start(Sock, Channel, FrameMax, Protocol),
    {ok, ChPid} = rabbit_framing_channel:start_link(
                    fun rabbit_channel:start_link/6,
                    [Channel, self(), WriterPid, Username, VHost, Collector],
                    Protocol),
    put({channel, Channel}, {chpid, ChPid}),
    put({chpid, ChPid}, {channel, Channel}),
    ok = rabbit_framing_channel:process(ChPid, AnalyzedFrame).

log_channel_error(ConnectionState, Channel, Reason) ->
    rabbit_log:error("connection ~p (~p), channel ~p - error:~n~p~n",
                     [self(), ConnectionState, Channel, Reason]).

handle_exception(State = #v1{connection_state = closed}, Channel, Reason) ->
    log_channel_error(closed, Channel, Reason),
    State;
handle_exception(State = #v1{connection_state = CS}, Channel, Reason) ->
    log_channel_error(CS, Channel, Reason),
    send_exception(State, Channel, Reason).

send_exception(State = #v1{connection = #connection{protocol = Protocol}},
               Channel, Reason) ->
    {ShouldClose, CloseChannel, CloseMethod} =
        map_exception(Channel, Reason, Protocol),
    NewState = case ShouldClose of
                   true  -> terminate_channels(),
                            close_connection(State);
                   false -> close_channel(Channel, State)
               end,
    ok = rabbit_writer:internal_send_command(
           NewState#v1.sock, CloseChannel, CloseMethod, Protocol),
    NewState.

map_exception(Channel, Reason, Protocol) ->
    {SuggestedClose, ReplyCode, ReplyText, FailedMethod} =
        lookup_amqp_exception(Reason, Protocol),
    ShouldClose = SuggestedClose or (Channel == 0),
    {ClassId, MethodId} = case FailedMethod of
                              {_, _} -> FailedMethod;
                              none   -> {0, 0};
                              _      -> Protocol:method_id(FailedMethod)
                          end,
    {CloseChannel, CloseMethod} =
        case ShouldClose of
            true -> {0, #'connection.close'{reply_code = ReplyCode,
                                            reply_text = ReplyText,
                                            class_id = ClassId,
                                            method_id = MethodId}};
            false -> {Channel, #'channel.close'{reply_code = ReplyCode,
                                                reply_text = ReplyText,
                                                class_id = ClassId,
                                                method_id = MethodId}}
        end,
    {ShouldClose, CloseChannel, CloseMethod}.

lookup_amqp_exception(#amqp_error{name        = Name,
                                  explanation = Expl,
                                  method      = Method},
                      Protocol) ->
    {ShouldClose, Code, Text} = Protocol:lookup_amqp_exception(Name),
    ExplBin = amqp_exception_explanation(Text, Expl),
    {ShouldClose, Code, ExplBin, Method};
lookup_amqp_exception(Other, Protocol) ->
    rabbit_log:warning("Non-AMQP exit reason '~p'~n", [Other]),
    {ShouldClose, Code, Text} = Protocol:lookup_amqp_exception(internal_error),
    {ShouldClose, Code, Text, none}.

amqp_exception_explanation(Text, Expl) ->
    ExplBin = list_to_binary(Expl),
    CompleteTextBin = <<Text/binary, " - ", ExplBin/binary>>,
    if size(CompleteTextBin) > 255 -> <<CompleteTextBin:252/binary, "...">>;
       true                        -> CompleteTextBin
    end.

internal_emit_stats(State) ->
    rabbit_event:notify(connection_stats,
                        [{Item, i(Item, State)} || Item <- ?STATISTICS_KEYS]).
