package org.pipelineframework.search.common.mapper;

import java.util.Currency;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
        return string != null ? java.util.Arrays.asList(string.split(",")) : null;
    }
}