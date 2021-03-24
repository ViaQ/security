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

public class IndexManagementTests extends SingleClusterTest {

    @Override
    protected String getResourceFolder() {
        return "openshift-logging";
    }

    @Test
    public void testDeleteIndex() throws Exception {
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

        res = rh.executeDeleteRequest(
                "app-000001",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-logging:elasticsearch"),
                new BasicHeader("x-forwarded-roles", "index-management")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains("{\"acknowledged\":true}"));
    }

    @Test
    public void testDeleteByQuery() throws Exception {
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

        res = rh.executePostRequest(
                "app-000001/_doc/_delete_by_query",
                "{\"query\":{\"match\":{ \"message\": \"This is just a test\"}}}",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-logging:elasticsearch"),
                new BasicHeader("x-forwarded-roles", "index-management")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains("{\"acknowledged\":true}"));
    }

    @Test
    public void testRollover() throws Exception {
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

        res = rh.executePostRequest(
                "app-write/_rollover",
                "",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "system:serviceaccount:openshift-logging:elasticsearch"),
                new BasicHeader("x-forwarded-roles", "index-management")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains("{\"acknowledged\":true,\"shards_acknowledged\":true,\"old_index\":\"app-000020\",\"new_index\":\"app-000021\",\"rolled_over\":true,\"dry_run\":false,\"conditions\":{}}"));
    }
}
