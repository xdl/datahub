package com.linkedin.metadata.models;

import com.datahub.test.BrowsePaths;
import com.datahub.test.Snapshot;
import com.datahub.test.TestEntityInfo;
import com.datahub.test.TestEntityKey;
import com.datahub.test.invalid.DuplicateSearchableFields;
import com.datahub.test.invalid.InvalidSearchableFieldType;
import com.datahub.test.invalid.MissingAspectAnnotation;
import com.datahub.test.invalid.MissingRelationshipName;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;
import static org.testng.Assert.*;


/**
 * Tests the capabilities of {@link EntitySpecBuilder}
 */
public class EntitySpecBuilderTest {

  @Test
  public void testBuildAspectSpecValidationAspectMissingAnnotation() {
    assertThrows(ModelValidationException.class, () ->
      new EntitySpecBuilder().buildAspectSpec(new MissingAspectAnnotation().schema())
    );
  }

  @Test
  public void testBuildAspectSpecValidationInvalidSearchableFieldType() {
    assertThrows(ModelValidationException.class, () ->
        new EntitySpecBuilder().buildAspectSpec(new InvalidSearchableFieldType().schema())
    );
  }

  @Test
  public void testBuildAspectSpecValidationDuplicateSearchableFields() {
    assertThrows(ModelValidationException.class, () ->
        new EntitySpecBuilder().buildAspectSpec(new DuplicateSearchableFields().schema())
    );
  }

  @Test
  public void testBuildAspectSpecValidationMissingRelationshipName() {
    assertThrows(ModelValidationException.class, () ->
        new EntitySpecBuilder().buildAspectSpec(new MissingRelationshipName().schema())
    );
  }

  @Test
  public void testBuildEntitySpecs() {

    // Instantiate the test Snapshot
    final Snapshot snapshot = new Snapshot();
    final List<EntitySpec> validEntitySpecs = new EntitySpecBuilder().buildEntitySpecs(snapshot.schema());

    // Assert single entity.
    assertEquals(1, validEntitySpecs.size());

    // Assert on Entity Spec
    final EntitySpec testEntitySpec = validEntitySpecs.get(0);
    assertEquals("testEntity", testEntitySpec.getName());

    // Assert on Aspect Specs
    final Map<String, AspectSpec> aspectSpecMap = testEntitySpec.getAspectSpecMap();
    assertEquals(3, aspectSpecMap.size());
    assertTrue(aspectSpecMap.containsKey("testEntityKey"));
    assertTrue(aspectSpecMap.containsKey("browsePaths"));
    assertTrue(aspectSpecMap.containsKey("testEntityInfo"));

    // Assert on TestEntityKey
    validateTestEntityKey(aspectSpecMap.get("testEntityKey"));

    // Assert on BrowsePaths Aspect
    validateBrowsePaths(aspectSpecMap.get("browsePaths"));

    // Assert on TestEntityInfo Aspect
    validateTestEntityInfo(aspectSpecMap.get("testEntityInfo"));
  }

  private void validateTestEntityKey(final AspectSpec keyAspectSpec) {
    assertEquals("testEntityKey", keyAspectSpec.getName());
    assertEquals(new TestEntityKey().schema().getFullName(), keyAspectSpec.getPegasusSchema().getFullName());

    // Assert on Searchable Fields
    assertEquals(2, keyAspectSpec.getSearchableFieldSpecs().size());
    assertEquals("keyPart1", keyAspectSpec.getSearchableFieldSpecMap().get(new PathSpec("keyPart1").toString())
        .getSearchableAnnotation().getFieldName());
    assertEquals(SearchableAnnotation.FieldType.TEXT, keyAspectSpec.getSearchableFieldSpecMap().get(new PathSpec("keyPart1").toString())
        .getSearchableAnnotation().getFieldType());
    assertEquals("keyPart3", keyAspectSpec.getSearchableFieldSpecMap().get(new PathSpec("keyPart3").toString())
        .getSearchableAnnotation().getFieldName());
    assertEquals(SearchableAnnotation.FieldType.KEYWORD, keyAspectSpec.getSearchableFieldSpecMap().get(new PathSpec("keyPart3").toString())
        .getSearchableAnnotation().getFieldType());

    // Assert on Relationship Field
    assertEquals(1, keyAspectSpec.getRelationshipFieldSpecs().size());
    assertEquals("keyForeignKey", keyAspectSpec.getRelationshipFieldSpecMap().get(new PathSpec("keyPart2").toString()).getRelationshipName());
  }


  private void validateBrowsePaths(final AspectSpec browsePathAspectSpec) {
    assertEquals("browsePaths", browsePathAspectSpec.getName());
    assertEquals(new BrowsePaths().schema().getFullName(), browsePathAspectSpec.getPegasusSchema().getFullName());
    assertEquals(1, browsePathAspectSpec.getSearchableFieldSpecs().size());
    assertEquals(SearchableAnnotation.FieldType.BROWSE_PATH, browsePathAspectSpec.getSearchableFieldSpecs().get(0)
        .getSearchableAnnotation().getFieldType());
  }

  private void validateTestEntityInfo(final AspectSpec testEntityInfo) {
    assertEquals("testEntityInfo", testEntityInfo.getName());
    assertEquals(new TestEntityInfo().schema().getFullName(), testEntityInfo.getPegasusSchema().getFullName());

    // Assert on Searchable Fields
    assertEquals(8, testEntityInfo.getSearchableFieldSpecs().size());
    assertEquals("customProperties", testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("customProperties").toString()).getSearchableAnnotation().getFieldName());
    assertEquals(SearchableAnnotation.FieldType.KEYWORD, testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("customProperties").toString())
        .getSearchableAnnotation().getFieldType());
    assertEquals("textFieldOverride", testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("textField").toString()).getSearchableAnnotation().getFieldName());
    assertEquals(SearchableAnnotation.FieldType.TEXT, testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("textField").toString())
        .getSearchableAnnotation().getFieldType());
    assertEquals("textArrayField", testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("textArrayField", "*").toString()).getSearchableAnnotation().getFieldName());
    assertEquals(SearchableAnnotation.FieldType.TEXT_PARTIAL, testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("textArrayField", "*").toString())
        .getSearchableAnnotation().getFieldType());
    assertEquals("nestedIntegerField", testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("nestedRecordField", "nestedIntegerField").toString()).getSearchableAnnotation().getFieldName());
    assertEquals(SearchableAnnotation.FieldType.COUNT, testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("nestedRecordField", "nestedIntegerField").toString())
        .getSearchableAnnotation().getFieldType());
    assertEquals("nestedArrayStringField", testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("nestedRecordArrayField", "*", "nestedArrayStringField").toString())
        .getSearchableAnnotation().getFieldName());
    assertEquals(SearchableAnnotation.FieldType.TEXT, testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("nestedRecordArrayField", "*", "nestedArrayStringField").toString())
        .getSearchableAnnotation().getFieldType());
    assertEquals("nestedArrayArrayField", testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("nestedRecordArrayField", "*", "nestedArrayArrayField", "*").toString())
        .getSearchableAnnotation().getFieldName());
    assertEquals(SearchableAnnotation.FieldType.TEXT, testEntityInfo.getSearchableFieldSpecMap().get(
        new PathSpec("nestedRecordArrayField", "*", "nestedArrayArrayField", "*").toString())
        .getSearchableAnnotation().getFieldType());

    // Assert on Relationship Fields
    assertEquals(4, testEntityInfo.getRelationshipFieldSpecs().size());
    assertEquals("foreignKey", testEntityInfo.getRelationshipFieldSpecMap().get(
        new PathSpec("foreignKey").toString()).getRelationshipName());
    assertEquals("foreignKeyArray", testEntityInfo.getRelationshipFieldSpecMap().get(
        new PathSpec("foreignKeyArray", "*").toString()).getRelationshipName());
    assertEquals("nestedForeignKey", testEntityInfo.getRelationshipFieldSpecMap().get(
        new PathSpec("nestedRecordField", "nestedForeignKey").toString()).getRelationshipName());
    assertEquals("nestedArrayForeignKey", testEntityInfo.getRelationshipFieldSpecMap().get(
        new PathSpec("nestedRecordArrayField", "*", "nestedArrayForeignKey").toString()).getRelationshipName());
  }

}
