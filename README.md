# What is it?

[![Gitter](https://badges.gitter.im/couchbase/couchbase-lite-java-core.svg)](https://gitter.im/couchbase/couchbase-lite-java-core?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

The [Couchbase Lite Android](https://github.com/couchbase/couchbase-lite-android) library is split into two parts:

* [couchbase-lite-java-core](https://github.com/couchbase/couchbase-lite-java-core) - this module, which has no dependencies on the Android API, and is usable in non-Android contexts.
* [couchbase-lite-android](https://github.com/couchbase/couchbase-lite-android) - which has dependencies on the Android API.

Likewise, the [Couchbase Lite Java](https://github.com/couchbase/couchbase-lite-java) library is split into two parts:

* [couchbase-lite-java-core](https://github.com/couchbase/couchbase-lite-java-core) - this module, which has no dependencies on the Android API, and is usable in non-Android contexts.
* [couchbase-lite-java-native](https://github.com/couchbase/couchbase-lite-java-native) - which contains a native wrapper of SQLite.

See the [Project Structure](https://github.com/couchbase/couchbase-lite-android/wiki/Project-structure) wiki page for more information.

# How to build

```
$ ./gradlew build
```

