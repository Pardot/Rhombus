package com.pardot.rhombus.web.odata;

import com.google.common.collect.Lists;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import org.odata4j.core.*;
import org.odata4j.edm.*;
import org.odata4j.exceptions.NotFoundException;
import org.odata4j.exceptions.NotImplementedException;
import org.odata4j.producer.*;
import org.odata4j.producer.edm.MetadataProducer;

import java.util.*;

/**
 * ReadOnly ODataProducer for Rhombus
 * Created by robrighter on 4/18/14.
 */
public class RODataProducer implements ODataProducer {

	protected ObjectMapper objectMapper;
	protected EdmDataServices metadata;
	protected NotFoundException.Factory notFoundFactory;

	public RODataProducer(ObjectMapper objectMapper){
		this.objectMapper = objectMapper;
		notFoundFactory = new NotFoundException.Factory();
		this.metadata = this.makeOdataSchemaFromRhombusKeyspace(objectMapper.getKeyspaceDefinition());
	}

	public EdmDataServices makeOdataSchemaFromRhombusKeyspace(CKeyspaceDefinition keyspace){
		String namespace = keyspace.getName();
		List<EdmEntityType.Builder> entityTypes = new ArrayList<EdmEntityType.Builder>();
		List<EdmEntitySet.Builder> entitySets = new ArrayList<EdmEntitySet.Builder>();
		for(CDefinition def : keyspace.getDefinitions().values()){
			EdmEntityType.Builder type = makeOdataTypeFromCDef(namespace, def);
			entityTypes.add(type);
			entitySets.add(EdmEntitySet.newBuilder().setName(def.getName()).setEntityType(type));
		}
		EdmEntityContainer.Builder container = EdmEntityContainer.newBuilder().setName(namespace + "Entities").setIsDefault(true).addEntitySets(entitySets);
		EdmSchema.Builder modelSchema = EdmSchema.newBuilder().setNamespace(namespace + "Model").addEntityTypes(entityTypes);
		EdmSchema.Builder containerSchema = EdmSchema.newBuilder().setNamespace(namespace + "Container").addEntityContainers(container);
		return EdmDataServices.newBuilder().addSchemas(containerSchema, modelSchema).build();
	}

	public EdmEntityType.Builder makeOdataTypeFromCDef(String namespace, CDefinition cdef){
		List<EdmProperty.Builder> properties = new ArrayList<EdmProperty.Builder>();
		for(CField f : cdef.getFields().values()){
			EdmType t = getOdataTypeFromCDataType(f.getType());
			properties.add(EdmProperty.newBuilder(f.getName()).setType(t));
		}
		return EdmEntityType.newBuilder().setNamespace(namespace).setName(cdef.getName()).addKeys("id").addProperties(properties);
	}

	public EdmType getOdataTypeFromCDataType(CField.CDataType rhombusType){
		switch (rhombusType) {
			case ASCII:
			case VARCHAR:
			case TEXT:
				return EdmSimpleType.STRING;
			case INT:
				return EdmSimpleType.INT32;
			case BIGINT:
			case COUNTER:
				return EdmSimpleType.INT64;
			case BLOB:
				throw new IllegalArgumentException();
			case BOOLEAN:
				return EdmSimpleType.BOOLEAN;
			case DECIMAL:
			case FLOAT:
			case DOUBLE:
				return EdmSimpleType.DOUBLE;
			case TIMESTAMP:
				return EdmSimpleType.DATETIMEOFFSET; //<= Not sure about this one
			case UUID:
			case TIMEUUID:
				return EdmSimpleType.GUID;
			case VARINT:
				return EdmSimpleType.INT64;
			default:
				throw new NotImplementedException("Datatype found in Rhombus definition has not been ported to the OData interface.");
		}
	}

	public OEntity makeOEntityFromRombusMap(EdmEntitySet ees, Map<String,Object> rhombusMap){
		List<OProperty<?>> properties = new ArrayList<OProperty<?>>();
		Object id = rhombusMap.get("id");
		for(String key: rhombusMap.keySet()){
			if(!key.equals("id")){
				properties.add(OProperties.simple(key,rhombusMap.get(key)));
			}
		}
		return OEntities.create(ees, OEntityKey.create(id), properties, null);
	}

	@Override
	public EdmDataServices getMetadata() {
		return this.metadata;
	}

	@Override
	public EntitiesResponse getEntities(String entitySetName, QueryInfo queryInfo) {
		//todo: respect:queryInfo.top
		//todo: respect:queryInfo.skip
		//todo: respect:queryInfo.orderBy
		//todo: respect:queryInfo.inlineCount <=maybe
		//todo: queryInfo.select
		throw new NotImplementedException();
	}

	@Override
	public EntityResponse getEntity(String entitySetName, OEntityKey entityKey, EntityQueryInfo queryInfo) {
		//todo: queryInfo.select
		Object key = null;
		try{
			Class keyClass = objectMapper.getKeyspaceDefinition().getDefinitions().get(entitySetName).getPrimaryKeyClass();
			if(keyClass.equals(UUID.class)){
				key = UUID.fromString(entityKey.asSingleValue().toString());
			}
			else if(keyClass.isAssignableFrom(Integer.class)){
				key = Integer.parseInt(entityKey.asSingleValue().toString());
			}
			else if(keyClass.isAssignableFrom(Long.class)){
				key = Long.parseLong(entityKey.asSingleValue().toString());
			}
			else if(keyClass.isAssignableFrom(String.class)){
				key = entityKey.asSingleValue().toString();
			}
			else {
				key = entityKey.asSingleValue();
			}
		}
		catch(Exception e){
			throw new IllegalArgumentException("Invalid Primary Key Type");
		}
		try {
			Map<String,Object> result = objectMapper.getByKey(entitySetName, key);
			if(result == null){
				throw notFoundFactory.createException(OErrors.error("404", "Cannot find object for key",""));
			}
			else{
				EdmEntitySet ees = getMetadata().getEdmEntitySet(entitySetName);
				//we found it, now convert it to a returnable type
				return Responses.entity(makeOEntityFromRombusMap(ees, result));
			}
		}
		catch(RhombusException e){
			throw new RuntimeException("Unable to perform database operation");
		}
	}

	@Override
	public CountResponse getEntitiesCount(String entitySetName, QueryInfo queryInfo) {
		throw new NotImplementedException();
	}

	@Override
	public EntitiesResponse getNavProperty(String entitySetName, OEntityKey entityKey, String navProp, QueryInfo queryInfo) {
		throw new NotImplementedException();
	}

	@Override
	public CountResponse getNavPropertyCount(String entitySetName, OEntityKey entityKey, String navProp, QueryInfo queryInfo) {
		throw new NotImplementedException();
	}

	@Override
	public void close() {}

	@Override
	public EntityResponse createEntity(String entitySetName, OEntity entity) {
		throw new NotImplementedException();
	}

	@Override
	public EntityResponse createEntity(String entitySetName, OEntityKey entityKey, String navProp, OEntity entity) {
		throw new NotImplementedException();
	}

	@Override
	public void deleteEntity(String entitySetName, OEntityKey entityKey) {
		throw new NotImplementedException();
	}

	@Override
	public void mergeEntity( String entitySetName, OEntity entity) {
		throw new NotImplementedException();
	}

	@Override
	public void updateEntity(String entitySetName, OEntity entity) {
		throw new NotImplementedException();
	}

	@Override
	public EntityIdResponse getLinks(OEntityId sourceEntity, String targetNavProp) {
		throw new NotImplementedException();
	}

	@Override
	public void createLink(OEntityId sourceEntity, String targetNavProp, OEntityId targetEntity) {
		throw new NotImplementedException();
	}

	@Override
	public void updateLink(OEntityId sourceEntity, String targetNavProp, OEntityKey oldTargetEntityKey, OEntityId newTargetEntity) {
		throw new NotImplementedException();
	}

	@Override
	public void deleteLink(OEntityId sourceEntity, String targetNavProp, OEntityKey targetEntityKey) {
		throw new NotImplementedException();
	}

	@Override
	public BaseResponse callFunction(EdmFunctionImport name, Map<String, OFunctionParameter> params, QueryInfo queryInfo) {
		throw new NotImplementedException();
	}

	@Override
	public MetadataProducer getMetadataProducer() {
		return null;
	}

	@Override
	public <TExtension extends OExtension<ODataProducer>> TExtension findExtension(Class<TExtension> clazz) {
		return null;
	}
}
