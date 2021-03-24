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

public class CollectorTests extends SingleClusterTest {

    @Override
    protected String getResourceFolder() {
        return "openshift-logging";
    }

    @Test
    public void testBulkIndexDocuments() throws Exception {
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
                "_bulk",
                "{ \"index\" : { \"_index\" : \"app-write\", \"_type\" : \"_doc\", \"_id\" : \"1\" } }\n" +
                        "{ \"index\" : { \"_index\" : \"app_write\", \"_type\" : \"_doc\", \"_id\" : \"2\" } }\n" +
                        "{ \"index\" : { \"_index\" : \"app-write\", \"_type\" : \"_doc\", \"_id\" : \"3\" } }\n" +
                        "{ \"index\" : { \"_index\" : \"app-write\", \"_type\" : \"_doc\", \"_id\" : \"4\" } }\n",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "CN=system.logging.fluentd,OU=Logging,O=OpenShift")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains("\"errors\":false,\"items\":[{\"index\":{\"_index\":\"app-000020\",\"_type\":\"_doc\",\"_id\":\"1\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":2,\"successful\":2,\"failed\":0},\"_seq_no\":1,\"_primary_term\":1,\"status\":201}},{\"index\":{\"_index\":\"app-000020\",\"_type\":\"_doc\",\"_id\":\"3\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":2,\"successful\":2,\"failed\":0},\"_seq_no\":2,\"_primary_term\":1,\"status\":201}}]"));
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
                "",
                // Use x-forwarded* headers to allow extended-proxy authentication
                new BasicHeader("x-forwarded-for", "127.0.0.1"),
                new BasicHeader("x-forwarded-user", "CN=system.logging.fluentd,OU=Logging,O=OpenShift")
        );

        System.out.println(res.getBody());
        Assert.assertEquals(HttpStatus.SC_OK, res.getStatusCode());
        Assert.assertTrue(res.getBody().contains("{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"app-000021\"}"));
    }
}
