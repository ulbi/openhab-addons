/*
 * Copyright (c) 2010-2026 Contributors to the openHAB project
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.openhab.persistence.timescaledb.internal;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.eclipse.jdt.annotation.DefaultLocation;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.openhab.core.items.Metadata;
import org.openhab.core.items.MetadataKey;
import org.openhab.core.items.MetadataRegistry;

/**
 * Unit tests for {@link TimescaleDBMetadataService}.
 *
 * @author René Ulbricht - Initial contribution
 */
@NonNullByDefault({ DefaultLocation.RETURN_TYPE, DefaultLocation.PARAMETER })
@SuppressWarnings("null")
class TimescaleDBMetadataServiceTest {

    private MetadataRegistry registry;
    private TimescaleDBMetadataService service;

    @BeforeEach
    void setUp() {
        registry = mock(MetadataRegistry.class);
        service = new TimescaleDBMetadataService(registry);
    }

    // ------------------------------------------------------------------
    // getDownsampleConfig — happy paths
    // ------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({ "AVG,1h,1 hour", "MAX,15m,15 minutes", "MIN,1d,1 day", "SUM,30m,30 minutes" })
    void getDownsampleConfigValidfunctionandinterval(String function, String interval, String expectedSql) {
        stubMetadata("MySensor", function, Map.of("downsampleInterval", interval));

        var config = service.getDownsampleConfig("MySensor");

        assertTrue(config.isPresent());
        assertEquals(AggregationFunction.valueOf(function), config.get().function());
        assertEquals(expectedSql, config.get().sqlInterval());
        assertEquals(5, config.get().retainRawDays()); // default
        assertEquals(0, config.get().retentionDays()); // default
    }

    @Test
    void getDownsampleConfigCustomretainrawandretentiondays() {
        stubMetadata("MySensor", "AVG",
                Map.of("downsampleInterval", "1h", "retainRawDays", "7", "retentionDays", "365"));

        var config = service.getDownsampleConfig("MySensor").orElseThrow();

        assertEquals(7, config.retainRawDays());
        assertEquals(365, config.retentionDays());
    }

    @Test
    void getDownsampleConfigAllsupportedintervals() {
        for (Map.Entry<String, String> entry : DownsampleConfig.INTERVAL_MAP.entrySet()) {
            stubMetadata("Item_" + entry.getKey(), "AVG", Map.of("downsampleInterval", entry.getKey()));
            var config = service.getDownsampleConfig("Item_" + entry.getKey());
            assertTrue(config.isPresent(), "Should parse interval: " + entry.getKey());
            assertEquals(entry.getValue(), config.get().sqlInterval());
        }
    }

    // ------------------------------------------------------------------
    // getDownsampleConfig — no / empty metadata
    // ------------------------------------------------------------------

    @Test
    void getDownsampleConfigNometadataReturnsempty() {
        when(registry.get(new MetadataKey("timescaledb", "Unknown"))).thenReturn(null);

        assertTrue(service.getDownsampleConfig("Unknown").isEmpty());
    }

    @Test
    void getDownsampleConfigBlankfunctionWithretentiondaysReturnsretentiononlyconfig() {
        // Blank value + retentionDays → retention-only config (no downsampling)
        stubMetadata("MySensor", " ", Map.of("retentionDays", "30"));

        Optional<DownsampleConfig> result = service.getDownsampleConfig("MySensor");
        assertTrue(result.isPresent());
        assertFalse(result.get().hasDownsampling());
        assertEquals(30, result.get().retentionDays());
        assertNull(result.get().function());
        assertNull(result.get().sqlInterval());
    }

    @Test
    void getDownsampleConfigBlankfunctionWithoutretentiondaysReturnsempty() {
        // Blank value + no retentionDays → nothing to do, skip item
        stubMetadata("MySensor", " ", Map.of());

        assertTrue(service.getDownsampleConfig("MySensor").isEmpty());
    }

    // ------------------------------------------------------------------
    // getDownsampleConfig — invalid / unsupported values
    // ------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = { "2h30m", "3m", "1w", "invalid", "" })
    void getDownsampleConfigInvalidintervalReturnsempty(String badInterval) {
        if (badInterval.isBlank()) {
            // handled by the missing-interval branch
            stubMetadata("MySensor", "AVG", Map.of());
        } else {
            stubMetadata("MySensor", "AVG", Map.of("downsampleInterval", badInterval));
        }

        assertTrue(service.getDownsampleConfig("MySensor").isEmpty());
    }

    @Test
    void getDownsampleConfigInvalidfunctionReturnsempty() {
        stubMetadata("MySensor", "MEDIAN", Map.of("downsampleInterval", "1h"));

        assertTrue(service.getDownsampleConfig("MySensor").isEmpty());
    }

    @Test
    void getDownsampleConfigMissingintervalReturnsempty() {
        // Function present but no interval → cannot downsample
        stubMetadata("MySensor", "AVG", Map.of());

        assertTrue(service.getDownsampleConfig("MySensor").isEmpty());
    }

    @Test
    void getDownsampleConfigInvalidretainrawdaysUsesdefault() {
        stubMetadata("MySensor", "AVG", Map.of("downsampleInterval", "1h", "retainRawDays", "not-a-number"));

        var config = service.getDownsampleConfig("MySensor").orElseThrow();
        assertEquals(5, config.retainRawDays()); // falls back to default
    }

    // ------------------------------------------------------------------
    // DownsampleConfig.toSqlInterval — allowlist enforcement
    // ------------------------------------------------------------------

    @Test
    void toSqlIntervalInvalidvalueThrowsillegalargument() {
        assertThrows(IllegalArgumentException.class, () -> DownsampleConfig.toSqlInterval("99h"));
    }

    @ParameterizedTest
    @CsvSource({ "1m,1 minute", "5m,5 minutes", "15m,15 minutes", "30m,30 minutes", "1h,1 hour", "6h,6 hours",
            "1d,1 day" })
    void toSqlIntervalValidvalues(String input, String expected) {
        assertEquals(expected, DownsampleConfig.toSqlInterval(input));
    }

    // ------------------------------------------------------------------
    // getConfiguredItemNames
    // ------------------------------------------------------------------

    @Test
    void getConfiguredItemNamesReturnsallTimescaledbitemsRegardlessofvalue() {
        Metadata withFunction = metadata("SensorA", "AVG", Map.of("downsampleInterval", "1h"));
        Metadata retentionOnly = metadata("SensorB", " ", Map.of("retentionDays", "30"));
        Metadata otherNamespace = new Metadata(new MetadataKey("influxdb", "SensorC"), "some", Map.of());

        when(registry.getAll()).thenAnswer(inv -> List.of(withFunction, retentionOnly, otherNamespace));

        List<String> names = service.getConfiguredItemNames();

        assertEquals(2, names.size());
        assertTrue(names.contains("SensorA"));
        assertTrue(names.contains("SensorB"));
        assertFalse(names.contains("SensorC"));
    }

    @Test
    void getConfiguredItemNamesEmptyregistryReturnsemptylist() {
        when(registry.getAll()).thenReturn(List.of());
        assertTrue(service.getConfiguredItemNames().isEmpty());
    }

    // ------------------------------------------------------------------
    // getUserTags — user-defined tag extraction
    // ------------------------------------------------------------------

    @Test
    void getUserTagsNometadataReturnsEmptyMap() {
        when(registry.get(new MetadataKey("timescaledb", "Unknown"))).thenReturn(null);

        assertTrue(service.getUserTags("Unknown").isEmpty());
    }

    @Test
    void getUserTagsOnlyReservedKeysReturnsEmptyMap() {
        // All config keys are reserved for downsampling — nothing should be returned as user tags
        stubMetadata("MySensor", "AVG",
                Map.of("downsampleInterval", "1h", "retainRawDays", "5", "retentionDays", "365"));

        assertTrue(service.getUserTags("MySensor").isEmpty());
    }

    @Test
    void getUserTagsNonreservedKeysAreReturned() {
        stubMetadata("MySensor", "AVG",
                Map.of("downsampleInterval", "1h", "room", "Corridor", "kind", "zigbee"));

        var tags = service.getUserTags("MySensor");

        assertEquals(2, tags.size());
        assertEquals("Corridor", tags.get("room"));
        assertEquals("zigbee", tags.get("kind"));
        assertFalse(tags.containsKey("downsampleInterval"), "Reserved key must not appear in user tags");
    }

    @Test
    void getUserTagsMixedKeysFiltersOutAllThreeReservedKeys() {
        stubMetadata("MySensor", " ",
                Map.of("retentionDays", "30", "retainRawDays", "5", "downsampleInterval", "1h",
                        "location", "indoors", "floor", "2"));

        var tags = service.getUserTags("MySensor");

        assertEquals(2, tags.size(), "Only non-reserved keys should be returned");
        assertEquals("indoors", tags.get("location"));
        assertEquals("2", tags.get("floor"));
    }

    @Test
    void getUserTagsValueIsNotIncludedInTags() {
        // The metadata value (aggregation function or blank) must never appear as a user tag
        stubMetadata("MySensor", "AVG", Map.of("room", "Kitchen"));

        var tags = service.getUserTags("MySensor");

        assertFalse(tags.containsKey("AVG"), "Metadata value must not appear as a user tag key");
        assertFalse(tags.containsValue("AVG"), "Metadata value must not appear as a user tag value");
        assertEquals(1, tags.size());
        assertEquals("Kitchen", tags.get("room"));
    }

    @Test
    void getUserTagsNullValueInConfigIsStoredAsEmptyString() {
        MetadataKey key = new MetadataKey("timescaledb", "MySensor");
        // Build a config map that contains a null value for a key
        var config = new java.util.HashMap<String, Object>();
        config.put("room", null);
        Metadata meta = new Metadata(key, "", config);
        when(registry.get(key)).thenReturn(meta);

        var tags = service.getUserTags("MySensor");

        assertTrue(tags.containsKey("room"), "Key with null value must still be present");
        assertEquals("", tags.get("room"), "Null value must be stored as empty string");
    }

    @Test
    void getUserTagsReturnedMapIsImmutable() {
        stubMetadata("MySensor", "AVG", Map.of("room", "Corridor"));

        var tags = service.getUserTags("MySensor");

        assertThrows(UnsupportedOperationException.class, () -> tags.put("newKey", "newVal"),
                "getUserTags must return an immutable map");
    }

    // ------------------------------------------------------------------
    // RESERVED_KEYS constant
    // ------------------------------------------------------------------

    @Test
    void reservedKeysContainsAllExpectedDownsamplingKeys() {
        assertTrue(TimescaleDBMetadataService.RESERVED_KEYS.contains("downsampleInterval"));
        assertTrue(TimescaleDBMetadataService.RESERVED_KEYS.contains("retainRawDays"));
        assertTrue(TimescaleDBMetadataService.RESERVED_KEYS.contains("retentionDays"));
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private void stubMetadata(String itemName, String value, Map<String, Object> config) {
        MetadataKey key = new MetadataKey("timescaledb", itemName);
        Metadata meta = metadata(itemName, value, config);
        when(registry.get(key)).thenReturn(meta);
    }

    private static Metadata metadata(String itemName, String value, Map<String, Object> config) {
        return new Metadata(new MetadataKey("timescaledb", itemName), value, config);
    }
}
