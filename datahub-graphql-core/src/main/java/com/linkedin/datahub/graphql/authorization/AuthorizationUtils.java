package com.linkedin.datahub.graphql.authorization;

import com.datahub.metadata.authorization.AuthorizationRequest;
import com.datahub.metadata.authorization.AuthorizationResult;
import com.datahub.metadata.authorization.Authorizer;
import com.datahub.metadata.authorization.ResourceSpec;
import java.util.Optional;
import javax.annotation.Nonnull;


public class AuthorizationUtils {

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup
  ) {
    return isAuthorized(authorizer, actor, Optional.empty(), privilegeGroup);
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull String resourceType,
      @Nonnull String resource,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup
  ) {
    final ResourceSpec resourceSpec = new ResourceSpec(resourceType, resource);
    return isAuthorized(authorizer, actor, Optional.of(resourceSpec), privilegeGroup);
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull Optional<ResourceSpec> maybeResourceSpec,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup
  ) {
    for (ConjunctivePrivilegeGroup andPrivilegeGroup : privilegeGroup.getAuthorizedPrivilegeGroups()) {
      // If any conjunctive privilege group is authorized, then the entire request is authorized.
      if (isAuthorized(authorizer, actor, andPrivilegeGroup, maybeResourceSpec)) {
        return true;
      }
    }
    // If none of the disjunctive privilege groups were authorized, then the entire request is not authorized.
    return false;
  }

  private static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull ConjunctivePrivilegeGroup requiredPrivileges,
      @Nonnull Optional<ResourceSpec> resourceSpec) {
    // Each privilege in a group _must_ all be true to permit the operation.
    for (final String privilege : requiredPrivileges.getRequiredPrivileges()) {
      // Create and evaluate an Authorization request.
      final AuthorizationRequest request = new AuthorizationRequest(actor, privilege, resourceSpec);
      final AuthorizationResult result = authorizer.authorize(request);
      if (AuthorizationResult.Type.DENY.equals(result.getType())) {
        // Short circuit.
        return false;
      }
    }
    return true;
  }

  private AuthorizationUtils() { }

}

