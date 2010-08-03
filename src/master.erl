-module(master).
-include_lib("deps/rabbitmq_erlang_client/include/amqp_client.hrl").

-export([start/0]). 
 
start() ->
    RID = amqp_connection:start_direct(),
    Channel = amqp_connection:open_channel(RID),

    X = <<"global_chat_exchange">>, 
    Q = <<"global_message_queue">>, 
    Key = <<"global_message_queue_publish_key">>,       %%our routing key, all clients have this

    amqp_channel:call(Channel, #'exchange.declare'{exchange = X, type = <<"topic">>, nowait = true}),
    amqp_channel:call(Channel, #'queue.declare'{queue = Q}),
    amqp_channel:call(Channel, #'queue.bind'{queue = Q, exchange = X, routing_key = Key}),
    
    io:fwrite("bound queue: ~p to exchange: ~p using key: ~p~n", [Q, X, Key]),
    
    amqp_channel:close(Channel),
    amqp_connection:close(RID).
