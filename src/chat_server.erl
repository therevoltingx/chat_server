-module(chat_server).
-include_lib("rabbitmq_erlang_client/include/amqp_client.hrl").
-compile(export_all).

start() ->
    RID = amqp_connection:start_direct(),
    Channel = amqp_connection:open_channel(RID),
     
    Queue = <<"global_message_queue">>,
    
    spawn( fun() -> consume_loop(RID, Channel, Queue) end ),
    self().
    
consume_loop(RID, Channel, Q) ->
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = Q}, self()),
    
    receive
         #'basic.consume_ok'{} ->
            io:fwrite("subscribed to queue: ~p listening for messages...~n", [Q])
    end,
    receive
        {#'basic.deliver'{delivery_tag=Tag}, Content} -> 
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
            handle_message(RID, Channel, Content),
            consume_loop(RID, Channel, Q);
        Unknown ->
            io:fwrite("unknown message: ~p tearing down~n", [Unknown]),
            teardown(RID, Channel)
    end.

handle_message(RID, Channel, Content) ->
    io:fwrite("got message: ~p from pid: ~p on channel: ~p ~n", [RID, Channel, Content]),
    todo.

teardown(RID, Channel) ->
    amqp_channel:close(Channel),
    amqp_connection:close(RID).
