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

public class ElasticsearchOperatorTests extends SingleClusterTest {

    @Override
    protected String getResourceFolder() {
        return "openshift-logging";
    }

    /**
     * Cluster Level Tests
     **/

    @Test
    public void testDoSynchronizedFlush() throws Exception {
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

        final RestHelper rh = nonSslRestHelper();
        RestHelper.HttpResponse res;

        res = rh.executeGetRequest(
                "_flush/synced",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-operators-redhat:elasticsearch-operator"),
                new BasicHeader("x-forwarded-roles", "elasticsearch-operator,prometheus"),
                new BasicHeader("x-ocp-ns", "openshift-logging")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
    }

    @Test
    public void testGetClusterHealth() throws Exception {
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

        final RestHelper rh = nonSslRestHelper();
        RestHelper.HttpResponse res;

        res = rh.executeGetRequest(
                "_cluster/health",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-operators-redhat:elasticsearch-operator"),
                new BasicHeader("x-forwarded-roles", "elasticsearch-operator,prometheus"),
                new BasicHeader("x-ocp-ns", "openshift-logging")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
    }

    @Test
    public void testGetClusterSettings() throws Exception {
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

        final RestHelper rh = nonSslRestHelper();
        RestHelper.HttpResponse res;

        res = rh.executeGetRequest(
                "_cluster/settings",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-operators-redhat:elasticsearch-operator"),
                new BasicHeader("x-forwarded-roles", "elasticsearch-operator,prometheus"),
                new BasicHeader("x-ocp-ns", "openshift-logging")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
    }

    @Test
    public void testGetClusterStateNodes() throws Exception {
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

        final RestHelper rh = nonSslRestHelper();
        RestHelper.HttpResponse res;

        res = rh.executeGetRequest(
                "_cluster/state/nodes/_all",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-operators-redhat:elasticsearch-operator"),
                new BasicHeader("x-forwarded-roles", "elasticsearch-operator,prometheus"),
                new BasicHeader("x-ocp-ns", "openshift-logging")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
    }

    @Test
    public void testGetClusterStats() throws Exception {
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

        final RestHelper rh = nonSslRestHelper();
        RestHelper.HttpResponse res;

        res = rh.executeGetRequest(
                "_cluster/stats",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-operators-redhat:elasticsearch-operator"),
                new BasicHeader("x-forwarded-roles", "elasticsearch-operator,prometheus"),
                new BasicHeader("x-ocp-ns", "openshift-logging")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
    }

    @Test
    public void testGetNodesStatsFS() throws Exception {
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

        final RestHelper rh = nonSslRestHelper();
        RestHelper.HttpResponse res;

        res = rh.executeGetRequest(
                "_nodes/stats/fs",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-operators-redhat:elasticsearch-operator"),
                new BasicHeader("x-forwarded-roles", "elasticsearch-operator,prometheus"),
                new BasicHeader("x-ocp-ns", "openshift-logging")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
    }

    /**
     * Index Level Tests
     **/

    @Test
    public void testGetSingleIndex() throws Exception {
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

        res = rh.executeGetRequest(
                "app-000010",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-operators-redhat:elasticsearch-operator"),
                new BasicHeader("x-forwarded-roles", "elasticsearch-operator,prometheus"),
                new BasicHeader("x-ocp-ns", "openshift-logging")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
    }

    @Test
    public void testGetAllIndices() throws Exception {
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

        res = rh.executeGetRequest(
                "_cat/indices/?format=json",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-operators-redhat:elasticsearch-operator"),
                new BasicHeader("x-forwarded-roles", "elasticsearch-operator,prometheus"),
                new BasicHeader("x-ocp-ns", "openshift-logging")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
    }

    @Test
    public void testGetAllIndicesForAlias() throws Exception {
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

        res = rh.executeGetRequest(
                "_alias/app-write",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-operators-redhat:elasticsearch-operator"),
                new BasicHeader("x-forwarded-roles", "elasticsearch-operator,prometheus"),
                new BasicHeader("x-ocp-ns", "openshift-logging")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains("{\"app-000020\":{\"aliases\":{\"app-write\":{}}}}"));
    }

    @Test
    public void testIndexSettings() throws Exception {
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

        res = rh.executeGetRequest(
                "app-000010/_settings",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-operators-redhat:elasticsearch-operator"),
                new BasicHeader("x-forwarded-roles", "elasticsearch-operator,prometheus"),
                new BasicHeader("x-ocp-ns", "openshift-logging")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
    }

    @Test
    public void testCreateIndex() throws Exception {
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

        res = rh.executePutRequest(
                "app-000021",
                "{\"aliases\":{\"app-write\":{\"is_write_index\":true}}}",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-operators-redhat:elasticsearch-operator"),
                new BasicHeader("x-forwarded-roles", "elasticsearch-operator,prometheus"),
                new BasicHeader("x-ocp-ns", "openshift-logging")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains("{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"app-000021\"}"));
    }

    @Test
    public void testUpdateIndexSettings() throws Exception {
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

        res = rh.executePutRequest(
                "app-000002/_settings",
                "{\"index\":{\"blocks\":{\"read_only_allow_delete\":true}}}",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-operators-redhat:elasticsearch-operator"),
                new BasicHeader("x-forwarded-roles", "elasticsearch-operator,prometheus"),
                new BasicHeader("x-ocp-ns", "openshift-logging")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains("{\"acknowledged\":true}"));
    }
}
