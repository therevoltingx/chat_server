-module(test_client).
-include_lib("deps/rabbitmq_erlang_client/include/amqp_client.hrl").

-export([say/1]). 

say(Msg) ->
    RID = amqp_connection:start_direct(),
    Channel = amqp_connection:open_channel(RID),

    X = <<"global_chat_exchange">>,
    Key = <<"global_message_queue_publish_key">>,
    
    Packet = list_to_binary(Msg),
    
    Publish = #'basic.publish'{exchange = X, routing_key = Key, mandatory=true, immediate=false},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = Packet}),
    
    amqp_channel:close(Channel),
    amqp_connection:close(RID).
