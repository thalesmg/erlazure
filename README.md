## erlazure (v0.2) [![Build Status](https://travis-ci.org/dkataskin/erlazure.svg?branch=master)](https://travis-ci.org/dkataskin/erlazure)

Erlazure is a library for accessing Windows Azure Storage Services. The API is subject to change.

Service APIs implemented:
* Queue storage service (API ver. "2014-02-14")
* Blob storage service (API ver. "2014-02-14")
* Table storage service (API ver. "2014-02-14")

## Requirements

Erlazure requires OTP version R16+.

## Implemented API functions
* Queue storage service
  * List queues
  * Get queue acl
  * Create queue
  * Delete queue
  * Put message
  * Get messages
  * Peek messages
  * Delete message
  * Clear messages
  * Update message

* Blob storage service
  * List containers
  * Create container
  * Delete container
  * Put block blob
  * Put page blob
  * List blobs
  * Get blob
  * Snapshot blob
  * Copy blob
  * Delete blob
  * Put block
  * Put block list
  * Get block list
  * Lease container

* Table storage service
  * List tables
  * New table
  * Delete table

## Starting an instance of erlazure

Start an instance of erlazure by calling `erlazure:new/2` where **Account** is Storage account name and **Key** is Storage account key.
```erlang
{ok, State} = erlazure:new(Account, Key)
```
Account and Key are strings.

### Using an emulated service like [Azurite](https://github.com/Azure/Azurite/blob/2bb552e703772b9a57ca713ef271c3c7c624a535/README.md)

```erlang
%% default dev credentials from Azurite
Account = "devstoreaccount1".
Key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".
%% Mind the trailing slash at the end of the endpoint.
{ok, State} = erlazure:new(#{account => Account, key => Key, endpoint => "http://127.0.0.1:10000/"})
```

## Calling Azure services
Almost each azure services request has three corresponding functions in ```erlazure``` module, the first has minimal set of parameters, the second has additionaly list of ```Options``` and the third has additionaly ```Timeout``` parameter.

```Options``` as the name states is list of options supported by this azure services request, each options is tuple ```{OptionName, OptionValue}``` where ```OptionName``` is atom and ```OptionValue``` can be of any type, option is passed either as a header or as a query string parameter.

```Timeout``` is number of milliseconds to wait for a reply from erlazure, infinity value is not supported. Note, that this is gen_server call timeout and isn't http request timeout (it's set to infinity by default) or azure service request timeout (you can specify it by sending option ```{timeout, _Timeout}```). By default timeout is set to 30 seconds.

For a list of supported options for each azure service request please consult msdn documentation.

## Examples

### Upload block blob
```
{ok, State} = erlazure:new("storage", "2o4b4tHpoWifLU+BlyzsIG1VtlO9LgBRFyl1qLw/+w9/ZszSxKGIK8JYac/UEJp5r8HKgiOiG8YTqGS9otAYWA=="),
{ok, Binary} = file:read("/path/to/some/small/file"),
{ok, created} = erlazure:put_block_blob(State, "uploads", "test_upload.file", Binary).
```
### Upload block blob with timeout set
Uploads block blob and waits no longer than 15 seconds for erlazure to finish the upload
```
{ok, State} = erlazure:new("storage", "2o4b4tHpoWifLU+BlyzsIG1VtlO9LgBRFyl1qLw/+w9/ZszSxKGIK8JYac/UEJp5r8HKgiOiG8YTqGS9otAYWA=="),
{ok, Binary} = file:read("/path/to/some/other/file"),
{ok, created} = erlazure:put_block_blob(State, "uploads", "test_upload2.file", Binary, [], 15000).
```

### Get 20 messages from a queue
Retrieves max 20 messages from a queue
```
{ok, State} = erlazure:new("storage", "2o4b4tHpoWifLU+BlyzsIG1VtlO9LgBRFyl1qLw/+w9/ZszSxKGIK8JYac/UEJp5r8HKgiOiG8YTqGS9otAYWA=="),
{ok, Messages} = erlazure:get_messages(State, "test_queue", [{num_of_messages, 20}]).
```

## License
Copyright © 2013–2015 Dmitriy Kataskin

Licensed under BSD License (see [LICENSE](license.txt)).
