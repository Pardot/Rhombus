package com.pardot.rhombus.web.odata;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.util.JsonUtil;
import org.joda.time.DateTime;
import org.odata4j.core.*;
import org.odata4j.edm.*;
import org.odata4j.exceptions.NotFoundException;
import org.odata4j.exceptions.NotImplementedException;
import org.odata4j.expression.*;
import org.odata4j.producer.*;
import org.odata4j.producer.edm.MetadataProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * ReadOnly ODataProducer for Rhombus
 * Created by robrighter on 4/18/14.
 */
public class RODataProducer implements ODataProducer {

	private static final Logger logger = LoggerFactory.getLogger(RODataProducer.class);

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
		EdmSchema.Builder modelSchema = EdmSchema.newBuilder().setNamespace(namespace).addEntityTypes(entityTypes);
		EdmSchema.Builder containerSchema = EdmSchema.newBuilder().setNamespace(namespace + "Container").addEntityContainers(container);
		return EdmDataServices.newBuilder().addSchemas(containerSchema, modelSchema).build();
	}

	public EdmEntityType.Builder makeOdataTypeFromCDef(String namespace, CDefinition cdef){
		List<EdmProperty.Builder> properties = new ArrayList<EdmProperty.Builder>();
		boolean idFound = false;
		for(CField f : cdef.getFields().values()){
			if(f.getName().equals("id")) {
				idFound = true;
			}
			EdmType t = getOdataTypeFromCDataType(f.getType());
			properties.add(EdmProperty.newBuilder(f.getName()).setType(t));
		}
		if(!idFound) {
			properties.add(EdmProperty.newBuilder("id").setType(EdmSimpleType.GUID));
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
		Set<String> keys = rhombusMap.keySet();
		Map<String, EdmProperty> propertyMap = getPropertyMap(ees);
		for(String key : keys ){
			Object value = rhombusMap.get(key);
			if(!key.equals("id")){
				try {
					EdmProperty property = propertyMap.get(key);
					if(property != null) {
						if(value != null && value instanceof Date) {
							value = new DateTime(value);
						}
						properties.add(OProperties.simple(key, (EdmSimpleType<Object>) property.getType(), value));
					}
				} catch (RuntimeException re) {
					logger.error("Exception adding property", re);
					throw re;
				}
			}
		}
		return OEntities.create(ees, OEntityKey.create(id), properties, null);
	}

	private Map<String, EdmProperty> getPropertyMap(EdmEntitySet ees) {
		Map<String, EdmProperty> propertyMap = Maps.newHashMap();
		for(EdmProperty property : ees.getType().getProperties()) {
			propertyMap.put(property.getName(), property);
		}
		return propertyMap;
	}

	@Override
	public EdmDataServices getMetadata() {
		logger.debug("getMetadata");
		return this.metadata;
	}

	protected List<OEntity> makeEntitiesList(String entitySetName, QueryInfo queryInfo) {
		logger.debug("makeEntitiesList {}", entitySetName);
		//todo: respect:queryInfo.top
		//todo: respect:queryInfo.skip
		//todo: respect:queryInfo.orderBy
		//todo: respect:queryInfo.inlineCount <=maybe
		//todo: queryInfo.select
		Criteria criteria = new Criteria();
		try {
            if (queryInfo.orderBy != null) {
                OrderByExpression orderByExpression = queryInfo.orderBy.get(0);
                String orderBy = ((EntitySimpleProperty) orderByExpression.getExpression()).getPropertyName();

                //criteria.setOrdering(orderBy);
            }
			if(queryInfo.top != null && queryInfo.top > 0) {
                logger.debug("Limit is set to " + queryInfo.top.toString());

                long limit = (long)queryInfo.top;
				criteria.setLimit(limit);
			} else {
				criteria.setLimit(200l);
			}
			if(queryInfo.skip != null && queryInfo.skip > 0) {
                logger.debug("Skip is set to " + queryInfo.skip.toString());
				// This is a major problem, can we just use skipToken?
			}
			if(queryInfo.filter != null) {
				BoolCommonExpression filter = queryInfo.filter;
				SortedMap<String, Object> indexKeys = Maps.newTreeMap();
				addIndexKeysFromFilter(filter, indexKeys);
				logger.debug("got index keys: {}", indexKeys);
				CDefinition cDef = objectMapper.getKeyspaceDefinition().getDefinitions().get(entitySetName);
				criteria.setIndexKeys(JsonUtil.rhombusMapFromJsonMap(indexKeys, cDef));
			}
		} catch(Exception e) {
			logger.error("Exception building criteria", e);
			throw new RuntimeException(e);
		}
		try {
			List<Map<String, Object>> results = objectMapper.list(entitySetName, criteria);
			logger.debug("Found results: {}", results);
			if(results == null){
				throw notFoundFactory.createException(OErrors.error("404", "Cannot find object for key",""));
			} else {
				EdmEntitySet ees = getMetadata().getEdmEntitySet(entitySetName);
				//we found it, now convert it to a returnable type
				List<OEntity> entities = Lists.newArrayList();
				for(Map<String, Object> result : results) {
					entities.add(makeOEntityFromRombusMap(ees, result));
				}
                return entities;
			}
		} catch(Exception e){
			logger.error("Error creating entity set", e);
			throw new RuntimeException("Unable to perform database operation");
		}
	}

    @Override
    public EntitiesResponse getEntities(String entitySetName, QueryInfo queryInfo) {
        logger.debug("getEntities {}", entitySetName);
        try {
            List<OEntity> entities = makeEntitiesList(entitySetName, queryInfo);

            EdmEntitySet ees = getMetadata().getEdmEntitySet(entitySetName);

            return Responses.entities(entities, ees, null, null);
        } catch(Exception e){
            logger.error("Error creating entity set", e);
            throw new RuntimeException("Unable to perform database operation");
        }
    }

    private void addIndexKeysFromFilter(BoolCommonExpression expression, SortedMap<String, Object> indexKeys) {
		// Assume we will only get and expressions containing field = value
		if (expression instanceof AndExpression) {
			AndExpression e = (AndExpression) expression;
			addIndexKeysFromFilter(e.getLHS(), indexKeys);
			addIndexKeysFromFilter(e.getRHS(), indexKeys);
		} else if(expression instanceof EqExpression) {
			EqExpression e = (EqExpression) expression;
			CommonExpression lhs = e.getLHS();
			if(lhs instanceof EntitySimpleProperty) {
				String field = ((EntitySimpleProperty)lhs).getPropertyName();
				CommonExpression rhs = e.getRHS();
				if(rhs instanceof LiteralExpression) {
					Object value = Expression.literalValue((LiteralExpression) rhs);
					indexKeys.put(field, value);
				}
			}
		}
	}

	@Override
	public EntityResponse getEntity(String entitySetName, OEntityKey entityKey, EntityQueryInfo queryInfo) {
		logger.debug("getEntity {} with id {}", entitySetName, entityKey);
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
		logger.debug("getEntitiesCount {}", entitySetName);

        List<OEntity> entities = makeEntitiesList(entitySetName, queryInfo);

        return Responses.count(entities.size());
	}

	@Override
	public EntitiesResponse getNavProperty(String entitySetName, OEntityKey entityKey, String navProp, QueryInfo queryInfo) {
		logger.debug("getNavProperty {}", entitySetName);
		throw new NotImplementedException();
	}

	@Override
	public CountResponse getNavPropertyCount(String entitySetName, OEntityKey entityKey, String navProp, QueryInfo queryInfo) {
		logger.debug("getNavPropertyCount {}", entitySetName);
		throw new NotImplementedException();
	}

	@Override
	public void close() {}

	@Override
	public EntityResponse createEntity(String entitySetName, OEntity entity) {
		logger.debug("createEntity {}", entitySetName);
		throw new NotImplementedException();
	}

	@Override
	public EntityResponse createEntity(String entitySetName, OEntityKey entityKey, String navProp, OEntity entity) {
		logger.debug("createEntity {}", entitySetName);
		throw new NotImplementedException();
	}

	@Override
	public void deleteEntity(String entitySetName, OEntityKey entityKey) {
		logger.debug("deleteEntity {}", entitySetName);
		throw new NotImplementedException();
	}

	@Override
	public void mergeEntity( String entitySetName, OEntity entity) {
		logger.debug("mergeEntity {}", entitySetName);
		throw new NotImplementedException();
	}

	@Override
	public void updateEntity(String entitySetName, OEntity entity) {
		logger.debug("updateEntity {}", entitySetName);
		throw new NotImplementedException();
	}

	@Override
	public EntityIdResponse getLinks(OEntityId sourceEntity, String targetNavProp) {
		logger.debug("getLinks");
		throw new NotImplementedException();
	}

	@Override
	public void createLink(OEntityId sourceEntity, String targetNavProp, OEntityId targetEntity) {
		logger.debug("createLink");
		throw new NotImplementedException();
	}

	@Override
	public void updateLink(OEntityId sourceEntity, String targetNavProp, OEntityKey oldTargetEntityKey, OEntityId newTargetEntity) {
		logger.debug("updateLink");
		throw new NotImplementedException();
	}

	@Override
	public void deleteLink(OEntityId sourceEntity, String targetNavProp, OEntityKey targetEntityKey) {
		logger.debug("deleteLink");
		throw new NotImplementedException();
	}

	@Override
	public BaseResponse callFunction(EdmFunctionImport name, Map<String, OFunctionParameter> params, QueryInfo queryInfo) {
		logger.debug("callFunction");
		throw new NotImplementedException();
	}

	@Override
	public MetadataProducer getMetadataProducer() {
		logger.debug("getMetadataProducer");
		return null;
	}

	@Override
	public <TExtension extends OExtension<ODataProducer>> TExtension findExtension(Class<TExtension> clazz) {
		logger.debug("findExtension");
		return null;
	}
}
