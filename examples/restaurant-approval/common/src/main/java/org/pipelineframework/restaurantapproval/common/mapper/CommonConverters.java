package org.pipelineframework.restaurantapproval.common.mapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Currency;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import org.mapstruct.Named;

public class CommonConverters {

    @Named("currencyToString")
    public String currencyToString(Currency currency) {
        return currency != null ? currency.getCurrencyCode() : null;
    }

    @Named("stringToCurrency")
    public Currency stringToCurrency(String code) {
        return code != null ? Currency.getInstance(code) : null;
    }

    @Named("uuidToString")
    public String uuidToString(UUID id) {
        return id != null ? id.toString() : null;
    }

    @Named("stringToUUID")
    public UUID stringToUUID(String value) {
        return value != null && !value.isBlank() ? UUID.fromString(value) : null;
    }

    @Named("bigDecimalToString")
    public String bigDecimalToString(BigDecimal value) {
        return value != null ? value.toPlainString() : null;
    }

    @Named("stringToBigDecimal")
    public BigDecimal stringToBigDecimal(String value) {
        return value != null && !value.isBlank() ? new BigDecimal(value) : null;
    }

    @Named("instantToString")
    public String instantToString(Instant value) {
        return value != null ? value.toString() : null;
    }

    @Named("stringToInstant")
    public Instant stringToInstant(String value) {
        return value != null && !value.isBlank() ? Instant.parse(value) : null;
    }

    @Named("atomicIntegerToString")
    public String atomicIntegerToString(AtomicInteger atomicInteger) {
        return atomicInteger != null ? String.valueOf(atomicInteger.get()) : null;
    }

    @Named("stringToAtomicInteger")
    public AtomicInteger stringToAtomicInteger(String string) {
        return string != null ? new AtomicInteger(Integer.parseInt(string)) : null;
    }

    @Named("atomicLongToString")
    public String atomicLongToString(AtomicLong atomicLong) {
        return atomicLong != null ? String.valueOf(atomicLong.get()) : null;
    }

    @Named("stringToAtomicLong")
    public AtomicLong stringToAtomicLong(String string) {
        return string != null ? new AtomicLong(Long.parseLong(string)) : null;
    }

    @Named("listToString")
    public String listToString(List<String> list) {
        return list != null ? String.join(",", list) : null;
    }

    @Named("stringToList")
    public List<String> stringToList(String string) {
        if (string == null) {
            return null;
        }
        if (string.equals("")) {
            return java.util.Collections.emptyList();
        }
        return java.util.Arrays.asList(string.split(","));
    }
}
