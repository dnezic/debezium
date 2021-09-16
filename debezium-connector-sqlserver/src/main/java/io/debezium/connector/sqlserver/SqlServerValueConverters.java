/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.ZonedTimestamp;

import microsoft.sql.DateTimeOffset;

/**
 * Conversion of SQL Server specific datatypes.
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerValueConverters extends JdbcValueConverters {

    static final long MILLISECONDS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);
    static final long MICROSECONDS_PER_SECOND = TimeUnit.SECONDS.toMicros(1);
    static final long MICROSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toMicros(1);
    static final long NANOSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
    static final long NANOSECONDS_PER_MICROSECOND = TimeUnit.MICROSECONDS.toNanos(1);
    static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
    static final long NANOSECONDS_PER_DAY = TimeUnit.DAYS.toNanos(1);
    static final long SECONDS_PER_DAY = TimeUnit.DAYS.toSeconds(1);
    static final long MICROSECONDS_PER_DAY = TimeUnit.DAYS.toMicros(1);
    static final LocalDate EPOCH = LocalDate.ofEpochDay(0);

    private ZoneId zoneId;

    public SqlServerValueConverters() {
    }

    /**
     * Create a new instance that always uses UTC for the default time zone when
     * converting values without timezone information to values that require
     * timezones.
     * <p>
     *
     * @param decimalMode
     *            how {@code DECIMAL} and {@code NUMERIC} values should be
     *            treated; may be null if
     *            {@link io.debezium.jdbc.JdbcValueConverters.DecimalMode#PRECISE}
     *            is to be used
     * @param temporalPrecisionMode
     *            date/time value will be represented either as Connect datatypes or Debezium specific datatypes
     */
    public SqlServerValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode,
                                    CommonConnectorConfig.BinaryHandlingMode binaryMode, ZoneId zoneId) {
        super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null, binaryMode);

        if (zoneId == null) {
            zoneId = ZoneId.of("UTC");
        }
        this.zoneId = zoneId;

    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        switch (column.jdbcType()) {
            // Numeric integers
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255, we thus need to store it in short int
                return SchemaBuilder.int16();

            // Floating point
            case microsoft.sql.Types.SMALLMONEY:
            case microsoft.sql.Types.MONEY:
                return SpecialValueDecimal.builder(decimalMode, column.length(), column.scale().get());
            case microsoft.sql.Types.DATETIMEOFFSET:
                return ZonedTimestamp.builder();
            default:
                return super.schemaBuilder(column);
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            // Numeric integers
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255, we thus need to store it in short int
                return (data) -> convertSmallInt(column, fieldDefn, data);

            // Floating point
            case microsoft.sql.Types.SMALLMONEY:
            case microsoft.sql.Types.MONEY:
                return (data) -> convertDecimal(column, fieldDefn, data);
            case Types.TIMESTAMP:

                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return data -> convertTimestampToEpochMillisTz(column, fieldDefn, data);
                    }
                    if (getTimePrecision(column) <= 6) {
                        return data -> convertTimestampToEpochMicrosTz(column, fieldDefn, data);
                    }
                    return (data) -> convertTimestampToEpochNanos(column, fieldDefn, data);
                }
                return (data) -> convertTimestampToEpochMillisAsDateTz(column, fieldDefn, data);

            case microsoft.sql.Types.DATETIMEOFFSET:
                return (data) -> convertTimestampWithZone(column, fieldDefn, data);

            // TODO Geometry and geography supported since 6.5.0
            default:
                return super.converter(column, fieldDefn);
        }
    }

    protected Object convertTimestampToEpochMillisAsDateTz(Column column, Field fieldDefn, Object data) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, new java.util.Date(0L), (r) -> {
            try {
                r.deliver(new java.util.Date(toEpochMillisTz(data, zoneId)));
            }
            catch (IllegalArgumentException e) {
            }
        });
    }

    private static long toEpochMicros(Instant instant) {
        return TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
    }

    private Object convertTimestampToEpochMillisTz(Column column, Field fieldDefn, Object data) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            try {
                r.deliver(toEpochMillisTz(data, zoneId));
            }
            catch (IllegalArgumentException e) {
            }
        });
    }

    private static long toEpochMillisTz(Object value, ZoneId zoneId) {
        if (value instanceof Long) {
            return (Long) value;
        }
        LocalDateTime dateTime = toLocalDateTime(value);
        Instant instant = dateTime.atZone(zoneId).toInstant();

        return instant.toEpochMilli();
    }

    private Object convertTimestampToEpochMicrosTz(Column column, Field fieldDefn, Object data) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            try {
                r.deliver(toEpochMicrosTz(data, zoneId));
            }
            catch (IllegalArgumentException e) {
            }
        });
    }

    private static long toEpochMicrosTz(Object value, ZoneId zoneId) {
        LocalDateTime dateTime = toLocalDateTime(value);
        Instant instant = dateTime.atZone(zoneId).toInstant();
        return toEpochMicros(instant);
    }

    private static LocalDateTime toLocalDateTime(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof OffsetDateTime) {
            return ((OffsetDateTime) obj).toLocalDateTime();
        }
        if (obj instanceof Instant) {
            return ((Instant) obj).atOffset(ZoneOffset.UTC).toLocalDateTime();
        }
        if (obj instanceof LocalDateTime) {
            return (LocalDateTime) obj;
        }
        if (obj instanceof LocalDate) {
            LocalDate date = (LocalDate) obj;
            return LocalDateTime.of(date, LocalTime.MIDNIGHT);
        }
        if (obj instanceof LocalTime) {
            LocalTime time = (LocalTime) obj;
            return LocalDateTime.of(EPOCH, time);
        }
        if (obj instanceof java.sql.Date) {
            java.sql.Date sqlDate = (java.sql.Date) obj;
            LocalDate date = sqlDate.toLocalDate();
            return LocalDateTime.of(date, LocalTime.MIDNIGHT);
        }
        if (obj instanceof java.sql.Time) {
            LocalTime localTime = toLocalTime(obj);
            return LocalDateTime.of(EPOCH, localTime);
        }
        if (obj instanceof java.sql.Timestamp) {
            java.sql.Timestamp timestamp = (java.sql.Timestamp) obj;
            return LocalDateTime.of(timestamp.getYear() + 1900,
                    timestamp.getMonth() + 1,
                    timestamp.getDate(),
                    timestamp.getHours(),
                    timestamp.getMinutes(),
                    timestamp.getSeconds(),
                    timestamp.getNanos());
        }
        if (obj instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) obj;
            long millis = (int) (date.getTime() % MILLISECONDS_PER_SECOND);
            if (millis < 0) {
                millis = MILLISECONDS_PER_SECOND + millis;
            }
            int nanosOfSecond = (int) (millis * NANOSECONDS_PER_MILLISECOND);
            return LocalDateTime.of(date.getYear() + 1900,
                    date.getMonth() + 1,
                    date.getDate(),
                    date.getHours(),
                    date.getMinutes(),
                    date.getSeconds(),
                    nanosOfSecond);
        }
        throw new IllegalArgumentException("Unable to convert to LocalTime from unexpected value '" + obj + "' of type " + obj.getClass().getName());
    }

    private static LocalTime toLocalTime(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof LocalTime) {
            return (LocalTime) obj;
        }
        if (obj instanceof LocalDateTime) {
            return ((LocalDateTime) obj).toLocalTime();
        }
        if (obj instanceof java.sql.Date) {
            throw new IllegalArgumentException("Unable to convert to LocalDate from a java.sql.Date value '" + obj + "'");
        }
        if (obj instanceof java.sql.Time) {
            java.sql.Time time = (java.sql.Time) obj;
            long millis = (int) (time.getTime() % MILLISECONDS_PER_SECOND);
            int nanosOfSecond = (int) (millis * NANOSECONDS_PER_MILLISECOND);
            return LocalTime.of(time.getHours(),
                    time.getMinutes(),
                    time.getSeconds(),
                    nanosOfSecond);
        }
        if (obj instanceof java.sql.Timestamp) {
            java.sql.Timestamp timestamp = (java.sql.Timestamp) obj;
            return LocalTime.of(timestamp.getHours(),
                    timestamp.getMinutes(),
                    timestamp.getSeconds(),
                    timestamp.getNanos());
        }
        if (obj instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) obj;
            long millis = (int) (date.getTime() % MILLISECONDS_PER_SECOND);
            int nanosOfSecond = (int) (millis * NANOSECONDS_PER_MILLISECOND);
            return LocalTime.of(date.getHours(),
                    date.getMinutes(),
                    date.getSeconds(),
                    nanosOfSecond);
        }
        if (obj instanceof Duration) {
            Long value = ((Duration) obj).toNanos();
            if (value >= 0 && value <= NANOSECONDS_PER_DAY) {
                return LocalTime.ofNanoOfDay(value);
            }
            else {
                throw new IllegalArgumentException("Time values must use number of milliseconds greater than 0 and less than 86400000000000");
            }
        }
        throw new IllegalArgumentException("Unable to convert to LocalTime from unexpected value '" + obj + "' of type " + obj.getClass().getName());
    }

    /**
     * Time precision in SQL Server is defined in scale, the default one is 7
     */
    @Override
    protected int getTimePrecision(Column column) {
        return column.scale().get();
    }

    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        if (!(data instanceof DateTimeOffset)) {
            return super.convertTimestampWithZone(column, fieldDefn, data);
        }
        final DateTimeOffset dto = (DateTimeOffset) data;

        // Timestamp is provided in UTC time
        final Timestamp utc = dto.getTimestamp();
        final ZoneOffset offset = ZoneOffset.ofTotalSeconds(dto.getMinutesOffset() * 60);
        return super.convertTimestampWithZone(column, fieldDefn, LocalDateTime.ofEpochSecond(utc.getTime() / 1000, utc.getNanos(), offset).atOffset(offset));
    }

}
