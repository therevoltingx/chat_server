.SUFFIXES: .erl .beam

.erl.beam:
	erlc -W $<

ARGS = -pa ebin \
       -pa deps/rabbit_common/ebin \
       -pa deps/rabbitmq_erlang_client/ebin \
       -boot start_sasl -s rabbit

ERL = erl ${ARGS}
ERLC = erlc -I . -I deps +debug_info -o ebin

MODS = src/master.erl src/chat_server.erl src/test_client.erl

all: 
	${ERLC} ${MODS}

run: 
	${ERL}

clean:
	rm -rf *.beam ebin/*.beam src/*.beam erl_crash.dump 
	
