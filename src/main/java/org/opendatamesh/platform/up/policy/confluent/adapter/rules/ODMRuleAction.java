package org.opendatamesh.platform.up.policy.confluent.adapter.rules;

import io.confluent.kafka.schemaregistry.rules.DlqAction;
import io.confluent.kafka.schemaregistry.rules.RuleAction;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.Multipart;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;

import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Instant;

public class ODMRuleAction implements RuleAction {

    private static final Logger logger = LoggerFactory.getLogger(ODMRuleAction.class);
    private static final String POLICY_TIMEFRAME_FIELD = "policy.timeframe";
    private static final String POLICY_MAIL_HOST_FIELD = "mail.smtp.host";
    private static final String POLICY_MAIL_PORT_FIELD = "mail.smtp.port";
    private static final String DLQ_TOPIC_FIELD = "dlq.topic";
    private static final String POLICY_MAIL_TO_FIELD = "owner_email";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String TYPE = "CUSTOMACTION";

    private static int countErrors = 0;
    private static int countSuccess = 0;
    private static Instant lastMailTimestamp; //last mail sent timestamp or timestamp of the first message produced (when the application is launched)

    private Map<String, ?> configs;
    private String username;
    private String password;

    public String type() {
        return TYPE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = configs;
        this.username = (String) configs.get(USERNAME);
        this.password = (String) configs.get(PASSWORD);
    }

    public void run(RuleContext ctx, Object message, RuleException ex)
            throws RuleException {
        String policyTimeframe = ctx.getParameter(POLICY_TIMEFRAME_FIELD);
        String dlqTopic = ctx.getParameter(DLQ_TOPIC_FIELD);

        if (policyTimeframe != null) {
            //messaggio in errore
            if(ex != null)
                countErrors++;
            else
                countSuccess ++;

            if (countErrors > 0 && checkTimeframe(policyTimeframe)) {
                sendMail(ctx, message, true);
                resetCountValues();
            }
            logger.info("Timeframe first timestamp: " + lastMailTimestamp.toString() +   " --> Error messages: " + countErrors + ", Ok messages: " + countSuccess);
        } else if(ex != null){
            logger.debug("NO timeframe set");
            sendMail(ctx, message, false);
        }

        if (ex != null) {
            if(dlqTopic != null){
                logger.warn("Writing to DLQ. Topic: " + dlqTopic);
                DlqAction action = new DlqAction();
                Map<String, Object> newConfig = (Map<String, Object>) configs;
                newConfig.put("topic", dlqTopic);
                action.configure(newConfig);
                action.run(ctx, message, ex);
            }
            throw ex;
        }
    }

    private boolean checkTimeframe(String minutes) throws RuleException {
        try {
            int minutesTimeframe = Integer.parseInt(minutes);
            if (lastMailTimestamp == null)
                lastMailTimestamp = Instant.now();
            //check if at least the timeframe has passed from last mail sent timestamp. If so the method returns true
            if (Instant.now().minus(minutesTimeframe, ChronoUnit.MINUTES).isAfter(lastMailTimestamp)) {
                return true;
            }
            return false;
        } catch (NumberFormatException e) {
            throw new RuleException("Parameter \"" + POLICY_TIMEFRAME_FIELD + "\" has a not valid value: " + minutes);
        }

    }

    private void resetCountValues(){
        lastMailTimestamp = Instant.now();
        countErrors = 0;
        countSuccess = 0;
    }

    private void sendMail(RuleContext ctx, Object message, boolean timeframe)
            throws RuleException {
        try {
            logger.info("Trying to send mail to notify the ERROR");
            String from = username;
            String to = ctx.getParameter(POLICY_MAIL_TO_FIELD);

            //Setting up properties for SMTP
            Properties props = new Properties();
            props.put("mail.transport.protocol", "smtp");
            props.put("mail.smtp.ssl.enable", true);
            props.put("mail.smtp.host", ctx.getParameter(POLICY_MAIL_HOST_FIELD));
            props.put("mail.smtp.port", ctx.getParameter(POLICY_MAIL_PORT_FIELD));
            props.put("mail.smtp.auth", "true");
            props.put("mail.smtp.starttls.enable", "true");
            Session session = Session.getInstance(props, new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password);
                }
            });

            Message mail = new MimeMessage(session);
            mail.setFrom(new InternetAddress(from));
            mail.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
            mail.setSubject("Confluent - message policy not validated");
            String msg;
            if(!timeframe) {
                msg = "Error in message: <br>'" + message
                        + "'.<br>It didn't respected the OPA policies associated to the topic";
            }
            else {
                msg = "In the following timeframe interval: " + lastMailTimestamp.toString() +  " - " + Instant.now().toString()
                        + " these are the statistics of the messages: "
                        + "\n - Policies OK: " + countSuccess
                        + "\n - Policies KO: " + countErrors;
            }

            MimeBodyPart mimeBodyPart = new MimeBodyPart();
            mimeBodyPart.setContent(msg, "text/html; charset=utf-8");
            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(mimeBodyPart);
            mail.setContent(multipart);

            Transport.send(mail);
            logger.info("Notification email sent");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuleException(e);
        }
    }
}
