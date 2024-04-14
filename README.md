# ZooKeeper API

A functional API layered over the ZooKeeper client.

The [original project](https://github.com/davidledwards/zookeeper), which contained both the API and CLI, was broken into two separate projects. The last commit before the original repository was cloned is [6ad2c34](https://github.com/davidledwards/zookeeper-client/commit/6ad2c34d799d4373aa077d89eecfde6e7e8e1612).

## Overview

As of version `1.7.1`, this library is published on [GitHub](https://github.com/davidledwards?tab=packages&repo_name=zookeeper-client), which is essentially a Maven-style repository. The aforementioned link provides dependency information. For versions prior to `1.7.1`, this library is published in the [Maven Central Repository](https://search.maven.org/search?q=g:com.loopfor.zookeeper).

API documentation for all versions can be found [here](https://davidedwards.io/zookeeper-client/).

## Releasing

Releases are published as GitHub Packages. The [release.sh](release.sh) script automates the entire process, which depends on the [GitHub CLI](https://cli.github.com/) being installed.

The version of the release is derived from the version specified in [build.sbt](build.sbt). A new package release will generate a corresponding tag, so the assumption is that the version number has been appropriately incremented. Otherwise, the release creation process will fail. This can happen if the version already exists as a package or the tag already exists.

If the release process is successful, a new tag of the format `v<version>` is created. For example, if the package version in `build.sbt` is `1.2.3`, then the corresponding tag is `v1.2.3`.

Once released, push all changes to GitHub.

```shell
$ git push origin master
$ git push --tags origin
```

## Documentation

API documentation is managed in the [docs][docs] folder and automatically published when changes are pushed to the `master` branch. Documentation is generated from `sbt`. Make sure all errors and warnings are eliminated.

```shell
sbt> doc
```

Create a new folder under [docs/api](docs/api) whose name matches the new version number. Then, copy the contents of the API documentation generated earlier under this new folder.

```shell
$ mkdir docs/api/<version>
$ cp -r target/scala-2.13/api/* docs/api/<version>
```

Add a link to the new version of documentation in [docs/index.html](docs/index.html). It should be intuitively obvious how to do this. When finished, commit and push changes.

## Contributing

Please refer to the [contribution guidelines](CONTRIBUTING.md) when reporting bugs and suggesting improvements.

## License

Copyright 2015 David Edwards

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

<https://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
