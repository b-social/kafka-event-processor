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
[b-social/kafka-event-processor "0.1.7"]
```

## Documentation

- [API Docs](http://b-social.github.io/kafka-event-processor)

## Usage

Run test with `lein test`

## Publishing

`kafka-event-processor` gets published to [clojars.org/b-social/kafka-event-processor](https://clojars.org/b-social/kafka-event-processor)
Which can be done following these steps:

1. Create an account on Clojars
2. Get added to the [b-social group](https://clojars.org/groups/b-social)

    - Current admins are: Elizabeth, Lucas, Natalia, Paul

3. Create a deploy key with b-social scope [here](https://clojars.org/tokens)
4. Commit the version change like [this](https://github.com/b-social/kafka-event-processor/commit/60aeef291673e21e16d9bbdbdee62cbbfbfa4dfe)
5. Run `lein test`
6. Run `lein deploy clojars`

    - Use your clojars deploy key, not password

7. Commit the snapshot version change like [this](https://github.com/b-social/kafka-event-processor/commit/34374d48fd2fdadda8fcd16078b2360d750ca760)

### Further reading

- [Clojars wiki - deploy tokens](https://github.com/clojars/clojars-web/wiki/Deploy-Tokens)
- [lein tutorial - snapshot versions](https://github.com/technomancy/leiningen/blob/stable/doc/TUTORIAL.md#snapshot-versions)
- [lein docs - deploy](https://github.com/technomancy/leiningen/blob/master/doc/DEPLOY.md#gpg)

## License

Copyright © 2020 Kroo Ltd

Distributed under the terms of the
[MIT License](http://opensource.org/licenses/MIT).
