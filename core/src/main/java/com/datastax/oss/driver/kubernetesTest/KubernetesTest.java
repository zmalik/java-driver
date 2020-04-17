/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.kubernetesTest;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.typesafe.config.ConfigFactory;
import edu.umd.cs.findbugs.annotations.NonNull;

public class KubernetesTest
{
    public static void main(String[] args)
    {
        //        CqlSession cqlSession = CqlSession.builder().addContactPoints(Arrays.asList(
        //                InetSocketAddress.createUnresolved("172.17.0.5", 9042)
        //                InetSocketAddress.createUnresolved("172.17.0.4", 9042))
        //        ).withLocalDatacenter("DC1-K8Demo").build();

        ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder = createDriverConfigLoader();
        configLoaderBuilder.withBoolean(DefaultDriverOption.RESOLVE_CONTACT_POINTS, true);

        CqlSession cqlSession = CqlSession.builder()
                .withConfigLoader(configLoaderBuilder.build())
                .addContactPoints(Collections.singletonList(
                InetSocketAddress.createUnresolved("10.105.230.229", 9042))
        ).withLocalDatacenter("DC1-K8Demo").build();

        System.out.println(cqlSession.getMetadata().getNodes());

        System.out.println("system.peers content:");
        List<Row> all = cqlSession.execute("select * from system.peers").all();
        System.out.println(all.stream().map(v -> v.getInetAddress("peer") + " " + v.getString("data_center") + " " + v.getUuid("host_id")).collect(Collectors.toList()));
    }


    private static ProgrammaticDriverConfigLoaderBuilder createDriverConfigLoader()
    {
        return new DefaultProgrammaticDriverConfigLoaderBuilder(
                () -> {
                    ConfigFactory.invalidateCaches();
                    return ConfigFactory.defaultOverrides()
                            .withFallback(ConfigFactory.parseResources("application.conf"))
                            .withFallback(ConfigFactory.parseResources("application.json"))
                            .withFallback(ConfigFactory.defaultReference())
                            .resolve();
                },
                DefaultDriverConfigLoader.DEFAULT_ROOT_PATH)
        {
            @NonNull
            @Override
            public DriverConfigLoader build()
            {
                return new NonReloadableDriverConfigLoader(super.build());
            }
        };
    }

    private static class NonReloadableDriverConfigLoader implements DriverConfigLoader
    {

        private final DriverConfigLoader delegate;

        public NonReloadableDriverConfigLoader(DriverConfigLoader delegate)
        {
            this.delegate = delegate;
        }

        @NonNull
        @Override
        public DriverConfig getInitialConfig()
        {
            return delegate.getInitialConfig();
        }

        @Override
        public void onDriverInit(@NonNull DriverContext context)
        {
            delegate.onDriverInit(context);
        }

        @NonNull
        @Override
        public CompletionStage<Boolean> reload()
        {
            return CompletableFutures.failedFuture(
                    new UnsupportedOperationException("reload not supported"));
        }

        @Override
        public boolean supportsReloading()
        {
            return false;
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }
}
