package it.unimore.dipi.openness.consumer;

import it.unimore.dipi.iot.openness.config.AuthorizedApplicationConfiguration;
import it.unimore.dipi.iot.openness.connector.EdgeApplicationAuthenticator;
import it.unimore.dipi.iot.openness.connector.EdgeApplicationConnector;
import it.unimore.dipi.iot.openness.dto.service.EdgeApplicationServiceDescriptor;
import it.unimore.dipi.iot.openness.dto.service.EdgeApplicationServiceList;
import it.unimore.dipi.iot.openness.exception.EdgeApplicationAuthenticatorException;
import it.unimore.dipi.iot.openness.exception.EdgeApplicationConnectorException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Optional;

/**
 * @author Marco Picone, Ph.D. - picone.m@gmail.com
 * @project openness-consumer-demo
 * @created 02/11/2020 - 15:41
 */
public class SimpleConsumerTester {

    public static final Logger logger = LoggerFactory.getLogger(SimpleConsumerTester.class);

    private static final String OPENNESS_CONTROLLER_BASE_AUTH_URL = "http://eaa.openness:7080/";

    private static final String OPENNESS_CONTROLLER_BASE_APP_URL = "https://eaa.openness:7443/";

    private static final String OPENNESS_CONTROLLER_BASE_APP_WS_URL = "wss://eaa.openness:7443/";

    private static final String APPLICATION_ID = "opennessConsumerDemo";

    private static final String NAME_SPACE = "consumerdemo";

    private static final String ORG_NAME = "DIPIUniMore";

    public static void main(String[] args) {

        try {

            logger.info("Starting Simple Openness Consumer Tester ...");

            AuthorizedApplicationConfiguration authorizedApplicationConfiguration = handleAuth();

            logger.info("Application Correctly Authenticated ! AppId: {}", authorizedApplicationConfiguration.getApplicationId());

            EdgeApplicationConnector edgeApplicationConnector = new EdgeApplicationConnector(
                    OPENNESS_CONTROLLER_BASE_APP_URL,
                    authorizedApplicationConfiguration,
                    OPENNESS_CONTROLLER_BASE_APP_WS_URL);

            //Activate Notification Channel
            ConsumerNotificationsHandler myNotificationsHandler = new ConsumerNotificationsHandler();
            edgeApplicationConnector.setupNotificationChannel(NAME_SPACE, APPLICATION_ID, myNotificationsHandler);

            EdgeApplicationServiceList availableServices = edgeApplicationConnector.getAvailableServices();

            if(availableServices != null && availableServices.getServiceList() != null){
                for(EdgeApplicationServiceDescriptor edgeApplicationServiceDescriptor : availableServices.getServiceList()){
                    logger.info("Service URN: {} -> {}", edgeApplicationServiceDescriptor.getServiceUrn(), edgeApplicationServiceDescriptor);

                    //Register to traffic information notification
                    if(edgeApplicationServiceDescriptor.getServiceUrn().getId().equals("opennessProducerDemoTraffic")) {
                        edgeApplicationConnector.postSubscription(edgeApplicationServiceDescriptor.getNotificationDescriptorList(),
                                edgeApplicationServiceDescriptor.getServiceUrn().getNamespace(),
                                edgeApplicationServiceDescriptor.getServiceUrn().getId());
                        final String endpoint = edgeApplicationServiceDescriptor.getEndpointUri();
                        getEvents(endpoint);
                    }
                }
            }
            else
                logger.error("EdgeApplicationServiceList = NULL or ServiceList = NULL !");

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private static void getEvents(final String endpoint) throws EdgeApplicationConnectorException {
        try{
            logger.debug("Get traffic events list - Target Url: {}", endpoint);
            HttpGet getEventList = new HttpGet(endpoint);
            final HttpClient httpClient = HttpClients.createDefault();
            HttpResponse response = httpClient.execute(getEventList);
            if(response != null && response.getStatusLine().getStatusCode() == 200){
                String bodyString = EntityUtils.toString(response.getEntity());
                logger.debug("Getting traffic events Response Code: {}", response.getStatusLine().getStatusCode());
                logger.debug("Response Body: {}", bodyString);
            } else {
                logger.error("Wrong Response Received !");
            }
        }catch (Exception e){
            String errorMsg = String.format("Error retrieving Service List ! Error: %s", e.getLocalizedMessage());
            logger.error(errorMsg);
            throw new EdgeApplicationConnectorException(errorMsg);
        }
    }

    private static AuthorizedApplicationConfiguration handleAuth() throws EdgeApplicationAuthenticatorException {

        final AuthorizedApplicationConfiguration authorizedApplicationConfiguration;

        final EdgeApplicationAuthenticator edgeApplicationAuthenticator = new EdgeApplicationAuthenticator(OPENNESS_CONTROLLER_BASE_AUTH_URL);

        final Optional<AuthorizedApplicationConfiguration> storedConfiguration = edgeApplicationAuthenticator.loadExistingAuthorizedApplicationConfiguration(APPLICATION_ID, ORG_NAME);

        if(storedConfiguration.isPresent()) {
            logger.info("AuthorizedApplicationConfiguration Loaded Correctly !");
            authorizedApplicationConfiguration = storedConfiguration.get();
        } else {
            logger.info("AuthorizedApplicationConfiguration Not Available ! Authenticating the app ...");
            authorizedApplicationConfiguration = edgeApplicationAuthenticator.authenticateApplication(NAME_SPACE, APPLICATION_ID, ORG_NAME);
        }

        return authorizedApplicationConfiguration;
    }

}
