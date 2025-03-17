package com.mrday.queryOutJena;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class App {
    // Endpoint di query di Fuseki (modifica se necessario)
    private static final String FUSEKI_QUERY_ENDPOINT = "http://localhost:3030/defaultOpenapi/query";
    // Namespace base per l'ontologia
    private static final String NS = "http://example.org/ontology#";

    public static void main(String[] args) throws Exception {
        // Imposta il server sulla porta 8066 (modifica se necessario)
        HttpServer server = HttpServer.create(new InetSocketAddress(8066), 0);
        server.createContext("/queryData", new QueryDataHandler());
        server.createContext("/stop", new StopHandler(server));
        server.setExecutor(null);
        server.start();
        System.out.println("Server in ascolto su http://localhost:8066/queryData");
        System.out.println("Per stoppare il server, invia una richiesta GET a http://localhost:8066/stop");
    }

    static class QueryDataHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws java.io.IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            // Recupera il parametro "q" dalla query string, se presente
            String queryParam = exchange.getRequestURI().getQuery();
            String sparqlQuery;
            if (queryParam != null && queryParam.contains("q=")) {
                sparqlQuery = URLDecoder.decode(queryParam.split("q=")[1], "UTF-8");
            } else {
                // Query di default: DESCRIBE tutte le istanze di getGraphOutput
                sparqlQuery = "PREFIX : <" + NS + "> " +
                              "DESCRIBE ?s WHERE { ?s a :getGraphOutput . }";
            }
            System.out.println("Esecuzione della query SPARQL:");
            System.out.println(sparqlQuery);
            
            // Esegue la query DESCRIBE su Fuseki usando Jena
            Model model;
            try (QueryExecution qexec = QueryExecutionFactory.sparqlService(FUSEKI_QUERY_ENDPOINT, sparqlQuery)) {
                model = qexec.execDescribe();
                System.out.println("Risultato RDF in TURTLE:");
                model.write(System.out, "TURTLE");
            }
            
            // Converte il modello RDF in JSON secondo lo schema OpenAPI desiderato
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode rootNode = mapper.createObjectNode();
            Resource mainInstance = findMainInstance(model);
            if (mainInstance != null) {
                ObjectNode mainJson = convertMainResourceToJson(mainInstance, model, mapper);
                rootNode.set("getGraphOutput", mainJson);
            } else {
                rootNode.putObject("getGraphOutput");
            }
            
            String jsonResponse = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
            System.out.println("Risposta JSON:");
            System.out.println(jsonResponse);
            
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            byte[] respBytes = jsonResponse.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, respBytes.length);
            OutputStream os = exchange.getResponseBody();
            os.write(respBytes);
            os.close();
        }
        
        // Trova l'istanza principale di getGraphOutput (che si assume abbia "_Instance" nell'URI)
        private static Resource findMainInstance(Model model) {
            ResIterator iter = model.listResourcesWithProperty(RDF.type, model.createResource(NS + "getGraphOutput"));
            while (iter.hasNext()) {
                Resource res = iter.nextResource();
                if (res.getURI() != null && res.getURI().contains("_Instance")) {
                    return res;
                }
            }
            if (iter.hasNext()) {
                return iter.nextResource();
            }
            return null;
        }
        
        /**
         * Converte la risorsa principale getGraphOutput in un ObjectNode.
         * La proprietà "results" viene convertita in un array di oggetti dettagliati.
         */
        private static ObjectNode convertMainResourceToJson(Resource res, Model model, ObjectMapper mapper) {
            ObjectNode node = mapper.createObjectNode();
            Map<String, List<RDFNode>> propMap = groupProperties(res);
            for (Map.Entry<String, List<RDFNode>> entry : propMap.entrySet()) {
                String key = entry.getKey();
                if ("results".equals(key)) {
                    ArrayNode resultsArr = mapper.createArrayNode();
                    for (RDFNode rn : entry.getValue()) {
                        if (rn.isResource()) {
                            ObjectNode resultJson = convertResourceToJson(rn.asResource(), mapper);
                            // Se il risultato è vuoto, prova a risalire al dettaglio tramite l'URI:
                            if (resultJson.size() == 0) {
                                String resURI = rn.asResource().getURI();
                                if (resURI != null && resURI.contains("results_")) {
                                    String detailedURI = resURI.replace("results_", "");
                                    Resource detailed = model.getResource(detailedURI);
                                    if (detailed != null && detailed.listProperties().hasNext()) {
                                        resultJson = convertResourceToJson(detailed, mapper);
                                    }
                                }
                            }
                            resultsArr.add(resultJson);
                        }
                    }
                    node.set("results", resultsArr);
                } else {
                    // Altre proprietà: se un solo valore, come campo semplice; se multipli, come array
                    if (entry.getValue().size() == 1) {
                        RDFNode rn = entry.getValue().get(0);
                        if (rn.isLiteral()) {
                            node.put(key, rn.asLiteral().getString());
                        } else if (rn.isResource()) {
                            node.set(key, convertResourceToJson(rn.asResource(), mapper));
                        }
                    } else {
                        ArrayNode arr = mapper.createArrayNode();
                        for (RDFNode rn : entry.getValue()) {
                            if (rn.isLiteral()) {
                                arr.add(rn.asLiteral().getString());
                            } else if (rn.isResource()) {
                                arr.add(convertResourceToJson(rn.asResource(), mapper));
                            }
                        }
                        node.set(key, arr);
                    }
                }
            }
            return node;
        }
        
        /**
         * Converte una risorsa RDF in un ObjectNode in modo ricorsivo.
         */
        private static ObjectNode convertResourceToJson(Resource res, ObjectMapper mapper) {
            ObjectNode node = mapper.createObjectNode();
            Map<String, List<RDFNode>> propMap = groupProperties(res);
            for (Map.Entry<String, List<RDFNode>> entry : propMap.entrySet()) {
                String key = entry.getKey();
                // Forziamo "results" e "versions" ad essere array
                if ("results".equals(key) || "versions".equals(key)) {
                    ArrayNode arr = mapper.createArrayNode();
                    for (RDFNode rn : entry.getValue()) {
                        if (rn.isResource()) {
                            arr.add(convertResourceToJson(rn.asResource(), mapper));
                        }
                    }
                    node.set(key, arr);
                } else {
                    if (entry.getValue().size() == 1) {
                        RDFNode rn = entry.getValue().get(0);
                        if (rn.isLiteral()) {
                            node.put(key, rn.asLiteral().getString());
                        } else if (rn.isResource()) {
                            node.set(key, convertResourceToJson(rn.asResource(), mapper));
                        }
                    } else {
                        ArrayNode arr = mapper.createArrayNode();
                        for (RDFNode rn : entry.getValue()) {
                            if (rn.isLiteral()) {
                                arr.add(rn.asLiteral().getString());
                            } else if (rn.isResource()) {
                                arr.add(convertResourceToJson(rn.asResource(), mapper));
                            }
                        }
                        node.set(key, arr);
                    }
                }
            }
            return node;
        }
        
        /**
         * Raggruppa per proprietà tutti i valori associati a una risorsa.
         */
        private static Map<String, List<RDFNode>> groupProperties(Resource res) {
            Map<String, List<RDFNode>> map = new HashMap<>();
            StmtIterator it = res.listProperties();
            while (it.hasNext()){
                Statement stmt = it.nextStatement();
                String prop = stmt.getPredicate().getLocalName();
                if ("type".equals(prop)) continue;
                map.computeIfAbsent(prop, k -> new ArrayList<>()).add(stmt.getObject());
            }
            return map;
        }
    }

    static class StopHandler implements HttpHandler {
        private final HttpServer server;
        public StopHandler(HttpServer server) { this.server = server; }
        @Override
        public void handle(HttpExchange exchange) throws java.io.IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            String response = "Server stoppato.";
            byte[] respBytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, respBytes.length);
            OutputStream os = exchange.getResponseBody();
            os.write(respBytes);
            os.close();
            System.out.println("Ricevuta richiesta per stoppare il server. Arresto in corso...");
            server.stop(0);
        }
    }
}
