package org.elasticsearch.common.settings;

import org.elasticsearch.common.inject.Inject;

/**
 *
 */
public class SettingsFilter extends ClientSettingsFilter {

    @Inject
    public SettingsFilter(Settings settings) {
        super(settings);
    }

}
