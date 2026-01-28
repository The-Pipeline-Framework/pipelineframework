---
search: false
---

# Java-Centered Type System

## Overview

The Pipeline Framework template generator now uses a Java DTO-centered approach instead of a protobuf-centered approach. This provides a much richer set of available types while maintaining compatibility with protobuf for communication.

## Key Changes

### 1. Java-First Type Selection
- Users select from rich Java types (LocalDateTime, BigDecimal, URI, etc.) instead of limited protobuf types
- Protobuf conversions happen automatically via MapStruct and custom converters
- Better developer experience with more appropriate domain types

### 2. Automatic Protobuf Mapping
- MapStruct automatically converts between Java DTOs and protobuf equivalents for built-in types
- Built-in conversions: primitives, wrappers, UUID, BigDecimal/BigInteger, Java 8 time types, URI/URL/File/Path
- Custom converters in `CommonConverters` class handle specialized mappings: Currency, AtomicInteger, AtomicLong, `List<String>`
- Null-safe conversions for all supported types

### 3. Expanded Type Support
The framework now supports a comprehensive set of Java types:

#### Basic Types
- String → protobuf `string`
- Integer → protobuf `int32`
- Long → protobuf `int64`
- Double → protobuf `double`
- Boolean → protobuf `bool`

#### Advanced Types
- UUID → protobuf `string`
- BigDecimal → protobuf `string`
- Currency → protobuf `string`
- Path → protobuf `string`
- BigInteger → protobuf `string`

#### Date/Time Types
- LocalDateTime → protobuf `string`
- LocalDate → protobuf `string`
- OffsetDateTime → protobuf `string`
- ZonedDateTime → protobuf `string`
- Instant → protobuf `int64` (epoch milliseconds)
- Duration → protobuf `int64` (nanoseconds)
- Period → protobuf `string`

#### Network Types
- URI → protobuf `string`
- URL → protobuf `string`
- File → protobuf `string`

#### Atomic Types
- AtomicInteger → protobuf `int32`
- AtomicLong → protobuf `int64`

#### Collection Types
- `List<String>` → protobuf `repeated string`

## Type Conversion System

### MapStruct Integration
- Automatic conversion between Java DTOs and gRPC protobuf messages
- Uses `@Named` converters from `CommonConverters` class
- Handles null-safe conversions automatically

### Custom Converters
The `CommonConverters` class provides:

#### Date/Time Converters
```java
@Named("localDateTimeToString")
public String localDateTimeToString(LocalDateTime localDateTime) { ... }

@Named("stringToLocalDateTime") 
public LocalDateTime stringToLocalDateTime(String string) { ... }
```

#### Collection Converters
```java
@Named("listToString")
public String listToString(List<String> list) { ... }

@Named("stringToList")
public List<String> stringToList(String string) { ... }
```

#### Specialized Converters
- UUID ↔ String
- BigDecimal ↔ String  
- Currency ↔ String
- Path ↔ String
- URI/URL ↔ String
- Atomic types conversions
- BigInteger ↔ String

## Updated Interactive Mode

When using the interactive mode, users now see:

```text
Available Java types with protobuf mappings:
  String -> string
  Integer -> int32
  Long -> int64
  Double -> double
  Boolean -> bool
  UUID -> string
  BigDecimal -> string
  Currency -> string
  Path -> string
  `List<String>` -> repeated string
  LocalDateTime -> string
  LocalDate -> string
  OffsetDateTime -> string
  ZonedDateTime -> string
  Instant -> int64
  Duration -> int64
  Period -> string
  URI -> string
  URL -> string
  File -> string
  BigInteger -> string
  AtomicInteger -> int32
  AtomicLong -> int64
```

## Benefits

### 1. Rich Domain Modeling
- Use appropriate Java types for domain concepts
- Better type safety and expressiveness
- More natural Java development experience

### 2. Automatic Serialization
- Complex types serialized to strings for protobuf compatibility
- Transparent conversion handled by MapStruct
- No manual conversion code needed

### 3. Extensibility
- Additional Java types can be added easily
- Custom converters support any type mapping
- Backward compatible with existing protobuf infrastructure

## Generated Code Improvements

### Enhanced Imports
- Automatic import generation based on used types
- Only imports necessary for the specific fields
- Clean, uncluttered generated code

### DTO and Domain Classes
- Proper imports for all Java types used
- Automatic date/time, collection, and specialized imports
- Clean separation between rich Java types and protobuf serialization

## Migration Notes

Existing YAML configurations will continue to work with the new system. The framework maintains backward compatibility while providing access to the expanded type system for new configurations.

To take advantage of the new types, simply use the updated type names when creating new pipeline configurations either through the interactive mode or by writing YAML files directly.

Example YAML:

```yaml
steps:
  - name: "ProcessPayment"
    inputTypeName: "PaymentInput"
    inputFields:
      - name: "paymentId"
        type: "UUID"
      - name: "amount"
        type: "BigDecimal"
      - name: "processedAt"
        type: "LocalDateTime"
      - name: "labels"
        type: "List<String>"
```
