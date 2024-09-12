package it.pagopa.pn.splitcon020.svc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.commons.exceptions.PnRuntimeException;
import it.pagopa.pn.splitcon020.dto.Con020InputEventDto;
import it.pagopa.pn.splitcon020.dto.Con020NewArchiveDto;
import org.springframework.stereotype.Service;

import java.util.Collections;

@Service
public class MessageConverterService {

    private final ObjectMapper objMapper;


    public MessageConverterService(ObjectMapper objMapper) {
        this.objMapper = objMapper;
    }

    public Con020InputEventDto parseInputEventFromJson( String jsonString ) {
        try {
            return objMapper.readValue( jsonString, Con020InputEventDto.class );
        } catch (JsonProcessingException exc ) {
            throw new PnRuntimeException( "Error parsing input event message", exc.getMessage(), 400, Collections.emptyList(), exc );
        }
    }

    public Con020NewArchiveDto parseNewArchiveEventFromJson(String jsonString ) {
        try {
            return objMapper.readValue( jsonString, Con020NewArchiveDto.class );
        } catch (JsonProcessingException exc ) {
            throw new PnRuntimeException( "Error parsing new archive event", exc.getMessage(), 400, Collections.emptyList(), exc );
        }
    }




}
