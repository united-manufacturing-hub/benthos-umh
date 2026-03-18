# Changelog

## [Unreleased]

## [0.12.0]

### Breaking Changes

- **S7comm address format updated for non-Data Block memory areas** - PE, PA, MK, C, and T areas no longer require a block number. Write `PE.X0.0` instead of `PE0.X0.0`. Data Block addresses (`DB1.DW20`) are unchanged. The old format needs to be updated to remove the block number

### Fixes

- **Fixed S7comm DateAndTime crash** - The `DateAndTime` data type previously caused a crash due to an incorrect buffer size and now reads correctly
- **Fixed false "required" warnings in the Management Console editor** - Fields with children that already have default values were incorrectly marked as required when editing bridge configurations
- **Fixed deprecated fields not shown as deprecated in the editor** - Fields marked as deprecated in bridge plugin definitions now correctly appear as deprecated in the Management Console editor
