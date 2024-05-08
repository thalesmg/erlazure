REBAR ?= rebar3

compile:
	$(REBAR) compile

.PHONY: eunit
eunit: compile
	$(REBAR) eunit -v -m erlazure,erlazure_xml_tests,erlazure_utils_tests,erlazure_queue_tests,erlazure_blob_tests
