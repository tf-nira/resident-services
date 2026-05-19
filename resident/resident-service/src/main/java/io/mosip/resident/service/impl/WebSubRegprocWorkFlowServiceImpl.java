package io.mosip.resident.service.impl;

import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.resident.config.LoggerConfiguration;
import io.mosip.resident.constant.EventStatusFailure;
import io.mosip.resident.constant.EventStatusInProgress;
import io.mosip.resident.constant.EventStatusSuccess;
import io.mosip.resident.constant.IdType;
import io.mosip.resident.constant.PacketStatus;
import io.mosip.resident.constant.RequestType;
import io.mosip.resident.constant.ResidentConstants;
import io.mosip.resident.constant.TemplateType;
import io.mosip.resident.dto.WorkflowCompletedEventDTO;
import io.mosip.resident.entity.ResidentTransactionEntity;
import io.mosip.resident.exception.ResidentServiceCheckedException;
import io.mosip.resident.repository.ResidentTransactionRepository;
import io.mosip.resident.service.WebSubRegprocWorkFlowService;
import io.mosip.resident.util.Utility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import java.util.Arrays;
import java.util.Set;

/**
 * @author Kamesh Shekhar Prasad
 */

@Component
public class WebSubRegprocWorkFlowServiceImpl implements WebSubRegprocWorkFlowService {

    private static final Logger logger = LoggerConfiguration.logConfig(WebSubRegprocWorkFlowServiceImpl.class);

    @Autowired
    Environment environment;

    @Autowired
    ResidentTransactionRepository residentTransactionRepository;

    @Autowired
    Utility utility;

    private static final Set<String> CONTACT_DETAIL_ATTRIBUTES = Set.of("email", "phone");

    @Override
    public void updateResidentStatus(WorkflowCompletedEventDTO workflowCompletedEventDTO) throws ResidentServiceCheckedException {
        logger.debug("WebSubRegprocWorkFlowServiceImpl:updateResidentStatus entry");
        ResidentTransactionEntity residentTransactionEntity = null;
        String individualId = null;
        if (workflowCompletedEventDTO.getResultCode() != null) {
            if (workflowCompletedEventDTO.getInstanceId() != null) {
                residentTransactionEntity =
                        residentTransactionRepository.findTopByAidOrderByCrDtimesDesc(workflowCompletedEventDTO.getInstanceId());
            }
            if (residentTransactionEntity != null) {
                individualId = residentTransactionEntity.getIndividualId();
                if (PacketStatus.getStatusCodeList(PacketStatus.FAILURE, environment).contains(workflowCompletedEventDTO.getResultCode())) {
                    utility.updateEntity(EventStatusFailure.FAILED.name(), RequestType.UPDATE_MY_UIN.name() + " - " + ResidentConstants.FAILED,
                            false, "Packet Failed in Regproc with status code-" +
                            workflowCompletedEventDTO.getResultCode(), residentTransactionEntity);
                    utility.sendNotification(residentTransactionEntity.getEventId(), individualId, TemplateType.REGPROC_FAILED);
                } else if (PacketStatus.getStatusCodeList(PacketStatus.SUCCESS, environment).contains(workflowCompletedEventDTO.getResultCode())) {
                    String statusCode = getStatusCodeForSuccessfulUpdate(residentTransactionEntity);
                    utility.updateEntity(statusCode, statusCode, false,
                            "Packet processed in Regproc with status code-" +
                            workflowCompletedEventDTO.getResultCode(), residentTransactionEntity);
                    utility.sendNotification(residentTransactionEntity.getEventId(), individualId, TemplateType.REGPROC_SUCCESS);
                }
            }
        }
        logger.debug("WebSubRegprocWorkFlowServiceImpl:updateResidentStatus exit");
    }

    private String getStatusCodeForSuccessfulUpdate(ResidentTransactionEntity residentTransactionEntity) {
        if (isContactDetailsUpdate(residentTransactionEntity.getAttributeList())) {
            return EventStatusSuccess.DATA_UPDATED.name();
        }
        return EventStatusInProgress.IDENTITY_UPDATED.name();
    }

    private boolean isContactDetailsUpdate(String attributeList) {
        if (attributeList == null || attributeList.trim().isEmpty()) {
            return false;
        }
        Set<String> updateAttributes = Arrays.stream(attributeList.split(ResidentConstants.SEMI_COLON))
                .map(String::trim)
                .filter(attribute -> !attribute.isEmpty())
                .filter(attribute -> !IdType.NIN.name().equalsIgnoreCase(attribute))
                .map(String::toLowerCase)
                .collect(java.util.stream.Collectors.toSet());
        return !updateAttributes.isEmpty() && CONTACT_DETAIL_ATTRIBUTES.containsAll(updateAttributes);
    }

}
