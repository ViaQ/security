package com.amazon.opendistroforelasticsearch.security.openshift_logging;

import com.amazon.opendistroforelasticsearch.security.test.DynamicSecurityConfig;
import com.amazon.opendistroforelasticsearch.security.test.SingleClusterTest;
import com.amazon.opendistroforelasticsearch.security.test.helper.rest.RestHelper;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KibanaTests extends SingleClusterTest {

    @Override
    protected String getResourceFolder() {
        return "openshift-logging";
    }

    @Test
    public void testProjectUserListIndicesForCreateIndexPattern() throws Exception {
        final Settings settings = Settings.builder()
                .put("opendistro_security.restapi.roles_enabled", "kibana_server")
                .build();

        final DynamicSecurityConfig dsc = new DynamicSecurityConfig()
                .setConfig("ocp_config.yml")
                .setSecurityRoles("ocp_roles.yml")
                .setSecurityRolesMapping("ocp_roles_mapping.yml")
                .setSecurityInternalUsers("ocp_internal_users.yml")
                .setSecurityActionGroups("ocp_action_groups.yml");

        setup(Settings.EMPTY, dsc, settings);

        try (TransportClient tc = getInternalTransportClient()) {
            Map indexSettings = new HashMap();
            indexSettings.put("number_of_shards", 1);
            indexSettings.put("number_of_replicas", 1);

            tc.admin().indices().create(new CreateIndexRequest(".kibana_1")
                            .alias(new Alias(".kibana"))
                            .settings(indexSettings))
                    .actionGet();

            tc.admin().indices().create(new CreateIndexRequest(".kibana_-1159834508_testuser1_2")
                            .settings(indexSettings))
                    .actionGet();

            tc.admin().indices().create(new CreateIndexRequest(".kibana_-1159834508_testuser1_3")
                            .alias(new Alias(".kibana_-1159834508_testuser1"))
                            .settings(indexSettings))
                    .actionGet();

            String kibUserBodyJSON = "{\"buildNum\" : 20385,\"defaultIndex\" : \"9f63ccf0-8cb1-11eb-994c-7551a3e70b0d\"}";
            tc.index(new IndexRequest(".kibana_-1159834508_testuser1_2")
                    .type("doc")
                    .id("6.8.1")
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(kibUserBodyJSON, XContentType.JSON)).actionGet();
            tc.index(new IndexRequest(".kibana_-1159834508_testuser1_3")
                    .type("doc")
                    .id("6.8.1")
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(kibUserBodyJSON, XContentType.JSON)).actionGet();

            for (int i = 1; i <= 2; i++) {
                String appIndexName = "app-00000" + i;
                CreateIndexRequest cir = new CreateIndexRequest(appIndexName)
                            .alias(new Alias("app"))
                            .settings(indexSettings);

                if (i == 2) {
                    cir = cir.alias(new Alias("app-write"));
                }
                tc.admin().indices().create(cir).actionGet();

                String doc = "{\"message\": \"This is just a test\", \"namespace\": \"test\", \"kubernetes\": {\"namespace_name\": \"test\"}}";
                tc.index(new IndexRequest(appIndexName)
                        .type("doc")
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(doc, XContentType.JSON)).actionGet();
            }
        }

        final RestHelper rh = nonSslRestHelper();
        RestHelper.HttpResponse res;

        res = rh.executePostRequest(
                "*/_search?ignore_unavailable=true",
            "{\n" +
                    "  \"size\": 0\n" +
                    "  ,\"aggs\": {\n" +
                    "    \"indices\": {\n" +
                    "      \"terms\": {\n" +
                    "        \"field\": \"_index\",\n" +
                    "        \"size\": 200\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "testuser1"),
                new BasicHeader("x-forwarded-roles", "project_user"),
                new BasicHeader("x-ocp-ns", "test"),

                // Fake Cookie storage for selected tenant in Kibana
                new BasicHeader("securitytenant", "__user__")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains("app-000001"));
        Assert.assertTrue(res.getBody().contains("app-000002"));
    }

    @Test
    public void testProjectUser() throws Exception {
        final Settings settings = Settings.builder()
                .put("opendistro_security.restapi.roles_enabled", "kibana_server")
                .build();

        final DynamicSecurityConfig dsc = new DynamicSecurityConfig()
                .setConfig("ocp_config.yml")
                .setSecurityRoles("ocp_roles.yml")
                .setSecurityRolesMapping("ocp_roles_mapping.yml")
                .setSecurityInternalUsers("ocp_internal_users.yml")
                .setSecurityActionGroups("ocp_action_groups.yml");

        setup(Settings.EMPTY, dsc, settings);

        try (TransportClient tc = getInternalTransportClient()) {
            Map indexSettings = new HashMap();
            indexSettings.put("number_of_shards", 1);
            indexSettings.put("number_of_replicas", 1);

            tc.admin().indices().create(new CreateIndexRequest(".kibana_1")
                            .alias(new Alias(".kibana"))
                            .settings(indexSettings))
                    .actionGet();

            tc.admin().indices().create(new CreateIndexRequest(".kibana_-1159834508_testuser1_2")
                            .settings(indexSettings))
                    .actionGet();

            tc.admin().indices().create(new CreateIndexRequest(".kibana_-1159834508_testuser1_3")
                            .alias(new Alias(".kibana_-1159834508_testuser1"))
                            .settings(indexSettings))
                    .actionGet();

            String kibUserBodyJSON = "{\"buildNum\" : 20385,\"defaultIndex\" : \"9f63ccf0-8cb1-11eb-994c-7551a3e70b0d\"}";
            tc.index(new IndexRequest(".kibana_-1159834508_testuser1_2")
                    .type("doc")
                    .id("6.8.1")
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(kibUserBodyJSON, XContentType.JSON)).actionGet();
            tc.index(new IndexRequest(".kibana_-1159834508_testuser1_3")
                    .type("doc")
                    .id("6.8.1")
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(kibUserBodyJSON, XContentType.JSON)).actionGet();

            for (int i = 1; i < 21; i++) {
                String leadingZeros = "00000";
                if (i >= 10) {
                    leadingZeros = "0000";
                }

                String appIndexName = "app-" + leadingZeros + i;
                CreateIndexRequest cir = new CreateIndexRequest(appIndexName)
                        .alias(new Alias("app"))
                        .settings(indexSettings);

                if (i == 20) {
                    cir = cir.alias(new Alias("app-write"));
                }
                tc.admin().indices().create(cir).actionGet();

                String doc = "{\"message\": \"This is just a test\", \"namespace\": \"test\", \"kubernetes\": {\"namespace_name\": \"test\"}}";
                tc.index(new IndexRequest(appIndexName)
                        .type("doc")
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(doc, XContentType.JSON)).actionGet();
            }

            for (int i = 1; i < 21; i++) {
                String leadingZeros = "00000";
                if (i >= 10) {
                    leadingZeros = "0000";
                }

                String appIndexName = "infra-" + leadingZeros + i;
                CreateIndexRequest cir = new CreateIndexRequest(appIndexName)
                        .alias(new Alias("infra"))
                        .settings(indexSettings);

                if (i == 20) {
                    cir = cir.alias(new Alias("infra-write"));
                }
                tc.admin().indices().create(cir).actionGet();

                String doc = "{\"message\": \"This is just a test\", \"namespace\": \"openshift-monitoring\", \"kubernetes\": {\"namespace_name\": \"openshift-monitoring\"}}";
                tc.index(new IndexRequest(appIndexName)
                        .type("doc")
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(doc, XContentType.JSON)).actionGet();
            }
        }

        final RestHelper rh = nonSslRestHelper();
        RestHelper.HttpResponse res;

        res = rh.executeGetRequest(
                "app-000001/_search",

                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "testuser1"),
                new BasicHeader("x-forwarded-roles", "project_user"),
                new BasicHeader("x-ocp-ns", "test"),

                // Fake Cookie storage for selected tenant in Kibana
                new BasicHeader("securitytenant", "__user__")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains("This is just a test"));

        res = rh.executeGetRequest(
                "infra-000001/_search",

                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "testuser1"),
                new BasicHeader("x-forwarded-roles", "project_user"),
                new BasicHeader("x-ocp-ns", "test"),

                // Fake Cookie storage for selected tenant in Kibana
                new BasicHeader("securitytenant", "__user__")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_FORBIDDEN, res.getStatusCode());
    }

    @Test
    public void testAdminUser() throws Exception {
        final Settings settings = Settings.builder()
                .put("opendistro_security.restapi.roles_enabled", "kibana_server")
                .build();

        final DynamicSecurityConfig dsc = new DynamicSecurityConfig()
                .setConfig("ocp_config.yml")
                .setSecurityRoles("ocp_roles.yml")
                .setSecurityRolesMapping("ocp_roles_mapping.yml")
                .setSecurityInternalUsers("ocp_internal_users.yml")
                .setSecurityActionGroups("ocp_action_groups.yml");

        setup(Settings.EMPTY, dsc, settings);

        try (TransportClient tc = getInternalTransportClient()) {
            Map indexSettings = new HashMap();
            indexSettings.put("number_of_shards", 1);
            indexSettings.put("number_of_replicas", 1);

            tc.admin().indices().create(new CreateIndexRequest(".kibana_1")
                            .alias(new Alias(".kibana"))
                            .settings(indexSettings))
                    .actionGet();

            tc.admin().indices().create(new CreateIndexRequest(".kibana_-1159834508_testuser1_2")
                            .settings(indexSettings))
                    .actionGet();

            tc.admin().indices().create(new CreateIndexRequest(".kibana_-1159834508_testuser1_3")
                            .alias(new Alias(".kibana_-1159834508_testuser1"))
                            .settings(indexSettings))
                    .actionGet();

            String kibUserBodyJSON = "{\"buildNum\" : 20385,\"defaultIndex\" : \"9f63ccf0-8cb1-11eb-994c-7551a3e70b0d\"}";
            tc.index(new IndexRequest(".kibana_-1159834508_testuser1_2")
                    .type("doc")
                    .id("6.8.1")
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(kibUserBodyJSON, XContentType.JSON)).actionGet();
            tc.index(new IndexRequest(".kibana_-1159834508_testuser1_3")
                    .type("doc")
                    .id("6.8.1")
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(kibUserBodyJSON, XContentType.JSON)).actionGet();

            for (int i = 1; i < 21; i++) {
                String leadingZeros = "00000";
                if (i >= 10) {
                    leadingZeros = "0000";
                }

                String appIndexName = "app-" + leadingZeros + i;
                CreateIndexRequest cir = new CreateIndexRequest(appIndexName)
                        .alias(new Alias("app"))
                        .settings(indexSettings);

                if (i == 20) {
                    cir = cir.alias(new Alias("app-write"));
                }
                tc.admin().indices().create(cir).actionGet();

                String doc = "{\"message\": \"This is just a test\", \"namespace\": \"test\", \"kubernetes\": {\"namespace_name\": \"test\"}}";
                tc.index(new IndexRequest(appIndexName)
                        .type("doc")
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(doc, XContentType.JSON)).actionGet();
            }

            for (int i = 1; i < 21; i++) {
                String leadingZeros = "00000";
                if (i >= 10) {
                    leadingZeros = "0000";
                }

                String appIndexName = "infra-" + leadingZeros + i;
                CreateIndexRequest cir = new CreateIndexRequest(appIndexName)
                        .alias(new Alias("infra"))
                        .settings(indexSettings);

                if (i == 20) {
                    cir = cir.alias(new Alias("infra-write"));
                }
                tc.admin().indices().create(cir).actionGet();

                String doc = "{\"message\": \"This is just a test\", \"namespace\": \"openshift-monitoring\", \"kubernetes\": {\"namespace_name\": \"openshift-monitoring\"}}";
                tc.index(new IndexRequest(appIndexName)
                        .type("doc")
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(doc, XContentType.JSON)).actionGet();
            }
        }

        final RestHelper rh = nonSslRestHelper();
        RestHelper.HttpResponse res;

        res = rh.executeGetRequest(
                "app-000001/_search",

                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "testuser1"),
                new BasicHeader("x-forwarded-roles", "admin_reader"),
                new BasicHeader("x-ocp-ns", "test"),

                // Fake Cookie storage for selected tenant in Kibana
                new BasicHeader("securitytenant", "__user__")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains("This is just a test"));

        res = rh.executeGetRequest(
                "infra-000001/_search",

                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "testuser1"),
                new BasicHeader("x-forwarded-roles", "admin_reader"),
                new BasicHeader("x-ocp-ns", "test"),

                // Fake Cookie storage for selected tenant in Kibana
                new BasicHeader("securitytenant", "__user__")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
    }

    @Test
    public void testKibanaUserTenantMigration() throws Exception {
        final Settings settings = Settings.builder()
                .put("opendistro_security.restapi.roles_enabled", "kibana_server")
                .build();

        final DynamicSecurityConfig dsc = new DynamicSecurityConfig()
                .setConfig("ocp_config.yml")
                .setSecurityRoles("ocp_roles.yml")
                .setSecurityRolesMapping("ocp_roles_mapping.yml")
                .setSecurityInternalUsers("ocp_internal_users.yml")
                .setSecurityActionGroups("ocp_action_groups.yml");

        setup(Settings.EMPTY, dsc, settings);

        try (TransportClient tc = getInternalTransportClient()) {
            Map indexSettings = new HashMap();
            indexSettings.put("number_of_shards", 1);
            indexSettings.put("number_of_replicas", 1);

            tc.admin().indices().create(new CreateIndexRequest(".kibana_1")
                            .alias(new Alias(".kibana"))
                            .settings(indexSettings))
                    .actionGet();

            tc.admin().indices().create(new CreateIndexRequest(".kibana_-1159834508_testuser1_2")
                            .settings(indexSettings))
                    .actionGet();

            tc.admin().indices().create(new CreateIndexRequest(".kibana_-1159834508_testuser1_3")
                            .alias(new Alias(".kibana_-1159834508_testuser1"))
                            .settings(indexSettings))
                    .actionGet();

            String kibUserBodyJSON = "{\"buildNum\" : 20385,\"defaultIndex\" : \"9f63ccf0-8cb1-11eb-994c-7551a3e70b0d\"}";
            tc.index(new IndexRequest(".kibana_-1159834508_testuser1_2")
                    .type("doc")
                    .id("6.8.1")
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(kibUserBodyJSON, XContentType.JSON)).actionGet();
            tc.index(new IndexRequest(".kibana_-1159834508_testuser1_3")
                    .type("doc")
                    .id("6.8.1")
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(kibUserBodyJSON, XContentType.JSON)).actionGet();

            for (int i = 1; i < 21; i++) {
                String leadingZeros = "00000";
                if (i >= 10) {
                    leadingZeros = "0000";
                }

                String appIndexName = "app-" + leadingZeros + i;
                CreateIndexRequest cir = new CreateIndexRequest(appIndexName)
                        .alias(new Alias("app"))
                        .settings(indexSettings);

                if (i == 20) {
                    cir = cir.alias(new Alias("app-write"));
                }
                tc.admin().indices().create(cir).actionGet();

                String doc = "{\"message\": \"This is just a test\", \"namespace\": \"test\", \"kubernetes\": {\"namespace_name\": \"test\"}}";
                tc.index(new IndexRequest(appIndexName)
                        .type("doc")
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(doc, XContentType.JSON)).actionGet();
            }
        }

        final RestHelper rh = nonSslRestHelper();
        RestHelper.HttpResponse res;

        res = rh.executeDeleteRequest(
                // Use .kibana for user testuser1, but this should be replaced by .kibana_-1159834508_testuser1
                // DELETE -> .kibana/doc/6.8.1 -> .kibana_-1159834508_testuser1/doc/6.8.1 -> .kibana_-1159834508_testuser1_3/doc/6.8.1
                ".kibana/doc/6.8.1",

                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "testuser1"),
                new BasicHeader("x-forwarded-roles", "project_user"),
                new BasicHeader("x-ocp-ns", "test"),

                // Fake Cookie storage for selected tenant in Kibana
                new BasicHeader("securitytenant", "__user__")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains(".kibana_-1159834508_testuser1"));
    }

    @Test
    public void testKibanaServerAccess() throws Exception {
        final Settings settings = Settings.builder()
                .put("opendistro_security.restapi.roles_enabled", "kibana_server")
                .build();

        final DynamicSecurityConfig dsc = new DynamicSecurityConfig()
                .setConfig("ocp_config.yml")
                .setSecurityRoles("ocp_roles.yml")
                .setSecurityRolesMapping("ocp_roles_mapping.yml")
                .setSecurityInternalUsers("ocp_internal_users.yml")
                .setSecurityActionGroups("ocp_action_groups.yml");

        setup(Settings.EMPTY, dsc, settings);

        try (TransportClient tc = getInternalTransportClient()) {
            Map indexSettings = new HashMap();
            indexSettings.put("number_of_shards", 1);
            indexSettings.put("number_of_replicas", 1);

            tc.admin().indices().create(new CreateIndexRequest(".kibana_1")
                            .alias(new Alias(".kibana"))
                            .settings(indexSettings))
                    .actionGet();

            String kibUserBodyJSON = "{\"buildNum\" : 20385,\"defaultIndex\" : \"9f63ccf0-8cb1-11eb-994c-7551a3e70b0d\"}";
            tc.index(new IndexRequest(".kibana")
                    .type("doc")
                    .id("6.8.1")
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(kibUserBodyJSON, XContentType.JSON)).actionGet();

            for (int i = 1; i < 21; i++) {
                String leadingZeros = "00000";
                if (i >= 10) {
                    leadingZeros = "0000";
                }

                String appIndexName = "app-" + leadingZeros + i;
                CreateIndexRequest cir = new CreateIndexRequest(appIndexName)
                        .alias(new Alias("app"))
                        .settings(indexSettings);

                if (i == 20) {
                    cir = cir.alias(new Alias("app-write"));
                }
                tc.admin().indices().create(cir).actionGet();

                String doc = "{\"message\": \"This is just a test\", \"namespace\": \"test\", \"kubernetes\": {\"namespace_name\": \"test\"}}";
                tc.index(new IndexRequest(appIndexName)
                        .type("doc")
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(doc, XContentType.JSON)).actionGet();
            }
        }

        final RestHelper rh = nonSslRestHelper();
        RestHelper.HttpResponse res;

        res = rh.executeDeleteRequest(
                ".kibana/doc/6.8.1",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "CN=system.logging.kibana,OU=Logging,O=OpenShift")
        );

        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
    }
}
