# b-social/kafka-event-processor

A libary to process kafka events making a number of choices

- Using stuartsierra components
- Event processing is wrapped in a jdbc transaction   
- Configured using environment variables from configurati
- Logs using cambium

Processors are configurable to whether the topic needs to be re-wound, there is idempotency and a callback when event processing is completed.

Allows for multiple processors configured in different ways. 

## Install

Add the following to your `project.clj` file:

```clj
[b-social/kafka-event-processor "0.1.6"]
```

## Documentation

* [API Docs](http://b-social.github.io/kafka-event-processor)

## Usage

FIXME

## License

Copyright © 2020 Kroo Ltd

Distributed under the terms of the 
[MIT License](http://opensource.org/licenses/MIT).
