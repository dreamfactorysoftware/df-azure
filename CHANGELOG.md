# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Added
### Changed
### Fixed

## [0.7.0] - 2017-01-16
### Changed
- Adhere to refactored df-core, see df-database and df-email
- Clean out use of MERGE verb, handled at router/controller level
- Cleanup schema management issues

## [0.6.0] - 2016-11-17
### Added
- DF-155 Adding Azure DocumentDB service type

### Changed
- DB base class changes to support field configuration across all database types.
- Base create and update table methods to allow for native settings

## [0.5.0] - 2016-10-03
### Changed
- DF-641 Download files in chunks
- DF-826 Protecting secret key using service config rework from df-core

## [0.4.0] - 2016-08-21
### Changed
- General cleanup from declaration changes in df-core for service doc and providers

## [0.3.1] - 2016-07-08
### Changed
- DF-763 Removed unsupported fields from API DOCs for NoSql services
- DF-749 ui tooltip text update
- Updating test cases

## [0.3.0] - 2016-05-27
### Changed
- Now using microsoft/azure-storage instead of the older sdk with pear requirements.
- Moved seeding functionality to service provider to adhere to df-core changes.

## [0.2.1] - 2016-04-22
### Changed
- Use new baseclass's trait instead of DBUtilities.

## [0.2.0] - 2016-01-29
### Changed
- **MAJOR** Updated code base to use OpenAPI (fka Swagger) Specification 2.0 from 1.2

## [0.1.2] - 2015-12-21
### Fixed
- Correct ColumnSchema usage for indexes

## [0.1.1] - 2015-12-18
### Changed
- Sync up with changes in df-core for schema classes

## 0.1.0 - 2015-10-24
First official release working with the new [df-core](https://github.com/dreamfactorysoftware/df-core) library.

[Unreleased]: https://github.com/dreamfactorysoftware/df-azure/compare/0.7.0...HEAD
[0.7.0]: https://github.com/dreamfactorysoftware/df-azure/compare/0.6.0...0.7.0
[0.6.0]: https://github.com/dreamfactorysoftware/df-azure/compare/0.5.0...0.6.0
[0.5.0]: https://github.com/dreamfactorysoftware/df-azure/compare/0.4.0...0.5.0
[0.4.0]: https://github.com/dreamfactorysoftware/df-azure/compare/0.3.1...0.4.0
[0.3.1]: https://github.com/dreamfactorysoftware/df-azure/compare/0.3.0...0.3.1
[0.3.0]: https://github.com/dreamfactorysoftware/df-azure/compare/0.2.1...0.3.0
[0.2.1]: https://github.com/dreamfactorysoftware/df-azure/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/dreamfactorysoftware/df-azure/compare/0.1.2...0.2.0
[0.1.2]: https://github.com/dreamfactorysoftware/df-azure/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/dreamfactorysoftware/df-azure/compare/0.1.0...0.1.1
