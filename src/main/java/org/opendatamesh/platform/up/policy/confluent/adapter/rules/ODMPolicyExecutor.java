package org.opendatamesh.platform.up.policy.confluent.adapter.rules;


import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.RuleExecutor;
import org.opendatamesh.platform.up.policy.api.v1.resources.ValidateResponse;
import org.opendatamesh.platform.up.policy.api.v1.resources.ValidatedPolicyResource;
import org.opendatamesh.platform.up.policy.opa.client.OpaClient;
import org.opendatamesh.platform.up.policy.opa.client.resources.v1.ValidateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ODMPolicyExecutor implements RuleExecutor {

    private static final Logger logger = LoggerFactory.getLogger(ODMPolicyExecutor.class);
    private static final String POLICY_NAMES_FIELD = "policy.names";
    private static final String POLICY_HOST_FIELD = "policy.host";
    private static final String POLICY_RANDOM_FIELD = "policy.random";

    private static final String URL_PATH = "/api/v1/planes/utility/policy-services/opa";
    public static final String TYPE = "CUSTOMEXECUTOR";

    public ODMPolicyExecutor() {
    }

    public String type(){
        return TYPE;
    }

    public Object transform(RuleContext ctx, Object message) throws RuleException{
        String policyNames = ctx.getParameter(POLICY_NAMES_FIELD);
        String policyHost = ctx.getParameter(POLICY_HOST_FIELD) + URL_PATH;
        String policyRandomlySelected = ctx.getParameter(POLICY_RANDOM_FIELD);

        logger.info("Verifying policies: " + policyNames);

        //String[] policiesArray = policyNames.split(",");

        if(policyRandomlySelected != null) {
            // if the message hasn't been selected by the random selection, the message doesn't need to be processed
            if(!randomlySelected(policyRandomlySelected)) {
                logger.debug("Message not selected for validation. Percentage: " + policyRandomlySelected);
                return message;
            }
        }

        String dataUrl = policyHost + "/validate";

        try {
            ObjectMapper mapper = new ObjectMapper();
            OpaClient opaClient = new OpaClient(policyHost, dataUrl + "?id=" + policyNames, 60L);
            ValidateRequest validateRequest = new ValidateRequest();
            validateRequest.setInput(message.toString());
            Map<String, Object> validationResult = opaClient.validateDocument(validateRequest);
            ValidateResponse validatePolicy = mapper.convertValue(validationResult, ValidateResponse.class);
            for (ValidatedPolicyResource validatedPolicy: validatePolicy.getValidatedPolicies()){
                if (!(boolean)((Map<String, Object>)((Map<String, Object>)validatedPolicy.getValidationResult()).get("result")).get("allow")){
                    throw new RuleException("Policy \"" + validatedPolicy.getPolicy() + "\" failed");
                }
                else {
                    logger.info("Policy \"" + validatedPolicy.getPolicy() + "\" validated");
                }
            }
            return message;
        }catch (Exception e){
            logger.error("Validation policy failed: " + e);
            throw new RuleException(e);
        }
    }

    // returns true if the message needs to be selected
    public boolean randomlySelected(String percentage) throws RuleException{
        try {
            float randomlySelectedPercentage = Float.parseFloat(percentage);
            if (randomlySelectedPercentage < 0.0 || randomlySelectedPercentage > 1.0)
                throw new NumberFormatException();

            // check if the message needs to be validated
            return Math.random() < randomlySelectedPercentage;
        } catch (NumberFormatException e) {
            throw new RuleException("Parameter \"" + POLICY_RANDOM_FIELD + "\" has a not valid value: " + percentage);
        }

    }
}