## Version 0.7.0 (2025/09/05)


### Pull Requests Merged

#### Features added

* [PR 72](https://github.com/pytroll/pytroll-watchers/pull/72) - Update python versions
* [PR 71](https://github.com/pytroll/pytroll-watchers/pull/71) - Remove Selector dependency on redis
* [PR 69](https://github.com/pytroll/pytroll-watchers/pull/69) - Add S3 polling backend
* [PR 51](https://github.com/pytroll/pytroll-watchers/pull/51) - Better exception handling when access to datastore is denied/rejected

In this release 4 pull requests were closed.


## Version 0.6.0 (2025/01/28)


### Pull Requests Merged

#### Features added

* [PR 50](https://github.com/pytroll/pytroll-watchers/pull/50) - Expand file metadata scope

In this release 1 pull request was closed.


## Version 0.5.0 (2025/01/22)

### Issues Closed

* [Issue 46](https://github.com/pytroll/pytroll-watchers/issues/46) - Improve documentation adding example on how to configure for watching for data in EUM Data Store ([PR 47](https://github.com/pytroll/pytroll-watchers/pull/47) by [@mraspaud](https://github.com/mraspaud))
* [Issue 42](https://github.com/pytroll/pytroll-watchers/issues/42) - Adding fields to message data ([PR 44](https://github.com/pytroll/pytroll-watchers/pull/44) by [@mraspaud](https://github.com/mraspaud))
* [Issue 41](https://github.com/pytroll/pytroll-watchers/issues/41) - URIs produced by pytroll-watches not understood by trollflow2/satpy ([PR 44](https://github.com/pytroll/pytroll-watchers/pull/44) by [@mraspaud](https://github.com/mraspaud))

In this release 3 issues were closed.

### Pull Requests Merged

#### Bugs fixed

* [PR 39](https://github.com/pytroll/pytroll-watchers/pull/39) - Suppress unwanted character encoding on local files

#### Features added

* [PR 47](https://github.com/pytroll/pytroll-watchers/pull/47) - Refactor to allow for the fetching capability ([46](https://github.com/pytroll/pytroll-watchers/issues/46))
* [PR 44](https://github.com/pytroll/pytroll-watchers/pull/44) - Remove filesystem info for local files by default ([42](https://github.com/pytroll/pytroll-watchers/issues/42), [41](https://github.com/pytroll/pytroll-watchers/issues/41))

In this release 3 pull requests were closed.


## Version 0.4.0 (2024/11/28)


### Pull Requests Merged

#### Bugs fixed

* [PR 29](https://github.com/pytroll/pytroll-watchers/pull/29) - Fix to many slashes in s3 path

#### Features added

* [PR 36](https://github.com/pytroll/pytroll-watchers/pull/36) - Fix filesystem serialisation to avoid connection
* [PR 31](https://github.com/pytroll/pytroll-watchers/pull/31) - Allow including directory name in file names
* [PR 30](https://github.com/pytroll/pytroll-watchers/pull/30) - Allow unpacking directories

#### Documentation changes

* [PR 27](https://github.com/pytroll/pytroll-watchers/pull/27) - Document the datastore watcher more

In this release 5 pull requests were closed.


## Version 0.3.0 (2024/08/22)


### Pull Requests Merged

#### Bugs fixed

* [PR 20](https://github.com/pytroll/pytroll-watchers/pull/20) - Fix tests for new version of fsspec

#### Features added

* [PR 25](https://github.com/pytroll/pytroll-watchers/pull/25) - Introduce a geo example for datastore and test
* [PR 17](https://github.com/pytroll/pytroll-watchers/pull/17) - Add unpacking possibilities to the watcher
* [PR 14](https://github.com/pytroll/pytroll-watchers/pull/14) - Use pre-commit.ci

In this release 4 pull requests were closed.


## Version 0.2.2 (2024/05/17)


### Pull Requests Merged

#### Features added

* [PR 13](https://github.com/pytroll/pytroll-watchers/pull/13) - Add selector feature

In this release 1 pull request was closed.


## Version 0.2.1 (2024/05/08)


### Pull Requests Merged

#### Bugs fixed

* [PR 12](https://github.com/pytroll/pytroll-watchers/pull/12) - Fix documentation to match reality
* [PR 11](https://github.com/pytroll/pytroll-watchers/pull/11) - Add classifiers and sdist build requirement

In this release 2 pull requests were closed.


## Version 0.2.0 (2024/05/08)


### Pull Requests Merged

#### Bugs fixed

* [PR 7](https://github.com/pytroll/pytroll-watchers/pull/7) - Fix missing uid in messages
* [PR 6](https://github.com/pytroll/pytroll-watchers/pull/6) - Log info when a message is sent

#### Features added

* [PR 10](https://github.com/pytroll/pytroll-watchers/pull/10) - Add sdist to pypi workflow
* [PR 9](https://github.com/pytroll/pytroll-watchers/pull/9) - Add a watcher for DHuS instances
* [PR 8](https://github.com/pytroll/pytroll-watchers/pull/8) - Add `path` to messages when `filesystem` is provided
* [PR 5](https://github.com/pytroll/pytroll-watchers/pull/5) - Add the datastore watcher
* [PR 4](https://github.com/pytroll/pytroll-watchers/pull/4) - Add copernicus dataspace watcher
* [PR 3](https://github.com/pytroll/pytroll-watchers/pull/3) - Add codecov, logging and aliasing
* [PR 2](https://github.com/pytroll/pytroll-watchers/pull/2) - Add cli and git-based version
* [PR 1](https://github.com/pytroll/pytroll-watchers/pull/1) - Refactor to use fix_times everywhere

In this release 10 pull requests were closed.
