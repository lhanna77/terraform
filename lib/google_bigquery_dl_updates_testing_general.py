from google.cloud import bigquery
import csv
import os
from datetime import datetime

os.environ["GCLOUD_PROJECT"] = "mstr-dwh-dev-83bf"

def run_query_and_save_to_csv(query, env, table):
    # Initialize BigQuery client
    client = bigquery.Client()
    
    # Run the query
    query_job = client.query(query)
    
    # Get query results
    results = query_job.result()
    
    # Generate a timestamp for the CSV file name
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Define the file path
    downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")
    csv_file_path = os.path.join(downloads_folder, f"{env}_{table}_{timestamp}.csv")
    
    # Write results to CSV file
    with open(csv_file_path, 'w', newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        
        # Write headers
        writer.writerow([field.name for field in results.schema])
        
        # Write rows
        for row in results:
            writer.writerow(row)
    
    return csv_file_path

def main():

    query_dict_list = [
    
    {"env" : "dev",
    "table" : "EmployerUser", ##### EmployerUser
    "enable" : "false",

    "query" : f"""

WITH cteJobsHistory as (
  SELECT 
    job.accountId AS ecomAccountId,
    creatorId.value AS userId,
    entitledSubscriptionPlan.value AS entitledSubscriptionPlan,
    job.jobStatus.status AS jobStatus,
    job.jobInactivationReason.name AS lastFreeInactiveReason,
    job.expiredDate AS lastFreeExpiredDate
  FROM
    `mstr-sevenseas-dev-cd2c.dl_jobs.history` jobsHistory
  INNER JOIN
    UNNEST (job.AttributeValuePairs) creatorId ON creatorId.name = 'creatorId'
  INNER JOIN
    UNNEST (job.AttributeValuePairs) entitledSubscriptionPlan ON entitledSubscriptionPlan.name = 'entitledSubscriptionPlan' AND entitledSubscriptionPlan.value = 'FREE'

  QUALIFY ROW_NUMBER() OVER (PARTITION BY job.jobId ORDER BY job.ModifiedDate DESC) = 1

),

cteActive AS (
  SELECT 
    DISTINCT ecomAccountId, 
    userId, 
    jobStatus 
  FROM 
    cteJobsHistory 
  WHERE 
    jobStatus = 'ACTIVE'
),

cteExpired AS (
  SELECT 
    ecomAccountId, 
    userId, 
    jobStatus,
    lastFreeInactiveReason,
    lastFreeExpiredDate
  FROM 
    cteJobsHistory 
  WHERE 
    jobStatus != 'ACTIVE'
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ecomAccountId, userId ORDER BY lastFreeExpiredDate DESC) = 1
)

SELECT
  UserAccount.email,
  '' as subscriberKey,
  '' as subscriberType,
  UserAccount.ecomAccountID,
  UserAccount.role,
  UserAccount.userId,
  UserAccount.userCreateDate,
  UserAccount.firstName,
  UserAccount.lastName,
  UserAccount.emailNotifications,
  UserAccount.languageTag,
  UserAccount.hasLoggedInBefore,
  UserAccount.isBlocked,
  UserAccount.userBlockedStatus,
  UserAccount.inactive,
  EmployerAccount.billingAccountId AS zuoraId,
  EmployerAccount.accountName,
  BillingAccount.crm_id AS sfdcAccountId,
  RatePlan.subscriptionId,
  RatePlan.productId,
  RatePlan.productName,
  RatePlan.subscriptionStatus,
  RatePlan.subscriptionStartDate,
  RatePlan.subscriptionEndDate,
  RatePlan.ratePlanAmendmentType,
  RatePlan.ratePlanCreatedDate,
  RatePlan.ratePlanUpdatedDate,

  IF (Active.jobStatus = 'ACTIVE',TRUE,FALSE) AS freeActiveJob,
  Expired.lastFreeInactiveReason,
  Expired.lastFreeExpiredDate--,
  --TIMESTAMP('{{var.value.v_sfmc_b2b_mart_etldate}}') as BQETLDate

FROM (
  SELECT
    User.EmployerUserEvent.email,
    COALESCE(Accounts.accountid, prevPlan.ecomAccountId) AS ecomAccountId,
    COALESCE(Accounts.role, prevPlan.role) AS role,
    User.EmployerUserEvent.user_id AS userId,
    User.EmployerUserEvent.app_metadata.userId AS appUserId,
    User.EmployerUserEvent.created_at_epoch_millis AS userCreateDate,
    User.EmployerUserEvent.first_name AS firstName,
    User.EmployerUserEvent.last_name AS lastName,
    User.EmployerUserEvent.user_metadata.emailNotifications,
    User.EmployerUserEvent.user_metadata.language AS languageTag,
    User.EmployerUserEvent.user_metadata.hasLoggedInBefore,
    IF(IFNULL(ARRAY_LENGTH(EmployerUserEvent.blocked_for),0)>0,TRUE,FALSE) AS isBlocked,
    ARRAY_TO_STRING(EmployerUserEvent.blocked_for, ', ') AS userBlockedStatus,
    IF(Accounts.accountid IS NULL AND prevPlan.ecomAccountId IS NOT NULL, TRUE, FALSE) AS inactive
  FROM
    `mstr-sevenseas-dev-cd2c.dl_employer.employer_user_events_history` User
  LEFT JOIN
    UNNEST(User.employeruserevent.app_metadata.accounts) AS Accounts
  LEFT JOIN (
    SELECT DISTINCT 
      User.EmployerUserEvent.user_id,
      Accounts.accountid AS ecomAccountId,
      Accounts.role
    FROM
      `mstr-sevenseas-dev-cd2c.dl_employer.employer_user_events_history` User
    INNER JOIN
      UNNEST(User.employeruserevent.app_metadata.accounts) AS Accounts
    ) prevPlan ON prevPlan.user_id = User.EmployerUserEvent.user_id and Accounts.accountid IS NULL
  WHERE
    User.EmployerUserEvent.email_verified = TRUE 
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY EmployerUserEvent.user_id, COALESCE(Accounts.accountid, prevPlan.ecomAccountId) ORDER BY User.serverEvent.eventTime DESC) = 1
) UserAccount

INNER JOIN (
  SELECT
    accountEvent.id AS ecomAccountId,
    accountEvent.billingAccountId,
    accountEvent.accountName,
  FROM
    `mstr-sevenseas-dev-cd2c.dl_employer.employer_account_history` 
  QUALIFY 
    ROW_NUMBER() OVER (PARTITION BY accountevent.id ORDER BY serverEvent.eventTime DESC) = 1

) EmployerAccount ON EmployerAccount.ecomAccountId = UserAccount.ecomAccountId

INNER JOIN
  `mstr-sevenseas-dev-cd2c.zuora.account` BillingAccount ON BillingAccount.id = EmployerAccount.billingAccountId

LEFT JOIN (
  SELECT
    RatePlan.account_id AS billingAccountId,
    subscription_id AS subscriptionId,
    Product.id AS productId,
    Product.name AS productName,
    Subscription.status AS subscriptionStatus,
    TIMESTAMP(Subscription.subscription_start_date) AS subscriptionStartDate,
    TIMESTAMP(Subscription.subscription_end_date) AS subscriptionEndDate,
    RatePlan.amendment_type AS ratePlanAmendmentType,
    RatePlan.created_date AS ratePlanCreatedDate,
    RatePlan.updated_date AS ratePlanUpdatedDate
  FROM
    `mstr-sevenseas-dev-cd2c.zuora.rate_plan` RatePlan
  INNER JOIN
    `mstr-sevenseas-dev-cd2c.zuora.product` Product ON Product.id = RatePlan.product_id
  INNER JOIN
    `mstr-sevenseas-dev-cd2c.zuora.subscription` Subscription ON Subscription.id = RatePlan.subscription_id
  WHERE
    UPPER(Product.name) IN ('STANDARD','PRO','FREE')
    AND Subscription.status = 'Active'
    AND COALESCE(RatePlan.amendment_type,'NewProduct') <> 'RemoveProduct'
    AND RatePlan._fivetran_deleted = FALSE
    AND Product._fivetran_deleted = FALSE
    AND Subscription._fivetran_deleted = FALSE 

  QUALIFY ROW_NUMBER() OVER (PARTITION BY RatePlan.account_id ORDER BY RatePlan.updated_date DESC) = 1
    
) RatePlan ON RatePlan.billingAccountId = EmployerAccount.billingAccountId

LEFT JOIN 
  cteActive Active ON Active.ecomAccountId = UserAccount.ecomAccountId AND Active.userId = UserAccount.appUserId

LEFT JOIN 
  cteExpired Expired ON Expired.ecomAccountId = UserAccount.ecomAccountId AND Expired.userId = UserAccount.appUserId

    """
    }
    ,
    {"env" : "prd",
    "table" : "AbandonedJobs", ##### AbandonedJobs
    "enable" : "false",

    "query" : f"""

SELECT
  EmployerUserAccountCurrent.userId,
  jobEvent.accountId AS ecomAccountId,
  RatePlan.productId,
  EmployerDraftJobHistoryCreated.firstCreatedDate,
  jobEvent.id AS jobId,
  jobEvent.title AS jobTitle,
  jobEvent.eventType,
  jobEvent.lastStepViewed,
  CASE 
    WHEN currentProductName = 'PRO' THEN 'UPGRADED'
    WHEN jobEvent.eventType = 'DraftPublishedEvent' THEN 'POSTED'
    WHEN jobEvent.eventType = 'DraftDeletedEvent' THEN 'DELETED'
    WHEN jobEvent.eventType = 'DraftUpdatedEvent' THEN 'UPDATED'
    WHEN jobEvent.eventType = 'DraftCreatedEvent' THEN 'CREATED'
  ELSE
    'UNKNOWN' END AS abandonedStatus,
  CASE 
    WHEN currentProductName = 'PRO' THEN RatePlan.ratePlanUpdatedDate
    WHEN jobEvent.eventType = 'DraftPublishedEvent' THEN EmployerDraftJobHistoryPublished.lastPostedDate
    WHEN jobEvent.eventType = 'DraftDeletedEvent' THEN EmployerDraftJobHistoryDeleted.lastDeletedDate
    WHEN jobEvent.eventType = 'DraftUpdatedEvent' THEN EmployerDraftJobHistoryUpdated.lastUpdatedDate
    WHEN jobEvent.eventType = 'DraftCreatedEvent' THEN EmployerDraftJobHistoryCreated.firstCreatedDate
  ELSE
    EmployerDraftJobHistoryCreated.firstCreatedDate
  END AS abandonedStatusUpdatedDate,
  EmployerUserAccountCurrent.email,
  EmployerUserAccountCurrent.firstName,
  EmployerUserAccountCurrent.lastName,
  EmployerUserAccountCurrent.languageTag,
  CASE 
    WHEN EmployerDraftJobHistoryCreated.firstCreatedDate = firstAbandoned.firstCreatedDate 
    THEN TRUE ELSE FALSE END AS usersFirstAbandonedJob,
  '' AS subscriberKey,
  '' AS subscriberType--,
  --TIMESTAMP('{{var.value.v_sfmc_b2b_mart_etldate}}') AS BQETLDate

FROM
  `mstr-sevenseas-prd-afea.dl_employer.employer_draft_job_history` EmployerDraftJobHistory

INNER JOIN (
  SELECT
    Accounts.accountId AS ecomAccountId,
    EmployerAccountCurrent.billingAccountId,
    EmployerAccountCurrent.createdByUserId,
    UserNames.userId,
    UserNames.email,
    UserNames.firstName,
    UserNames.lastName,
    EmployerUserEventsCurrent.UserMetadata.Language AS languageTag
  FROM
    `mstr-dwh-prd-4e3e.Employer.EmployerUserEventsCurrent` EmployerUserEventsCurrent
  INNER JOIN
    UNNEST(AppMetadata.accounts) AS Accounts
  LEFT JOIN
    `mstr-dwh-prd-4e3e.Employer.EmployerAccountCurrent` AS EmployerAccountCurrent ON EmployerAccountCurrent.EmployerAccountID = Accounts.AccountID  
  LEFT JOIN
    (SELECT
      Accounts.accountId AS ecomAccountId,
      EmployerUserEvent.user_id AS userId,
      EmployerUserEvent.email,
      EmployerUserEvent.first_name AS firstName,
      EmployerUserEvent.last_name AS lastName
    FROM
      `mstr-sevenseas-prd-afea.dl_employer.employer_user_events_history`
    INNER JOIN
      UNNEST(employeruserevent.app_metadata.accounts) AS Accounts
    WHERE 
      EmployerUserEvent.email_verified = TRUE 
    AND
      EmployerUserEvent.email IS NOT NULL
      
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Accounts.accountId ORDER BY serverEvent.eventTime DESC) = 1

    ) AS UserNames ON UserNames.ecomAccountId = Accounts.accountId

) EmployerUserAccountCurrent ON EmployerUserAccountCurrent.ecomAccountId = EmployerDraftJobHistory.jobEvent.accountId AND EmployerUserAccountCurrent.createdByUserId = EmployerDraftJobHistory.jobEvent.creatorId

LEFT JOIN (
  SELECT
    jobEvent.id AS Id,
    MIN(jobEvent.createdDate) AS firstCreatedDate
  FROM
    `mstr-sevenseas-prd-afea.dl_employer.employer_draft_job_history` 
  WHERE
    jobEvent.eventType = 'DraftCreatedEvent'
  GROUP BY
    id
) EmployerDraftJobHistoryCreated ON EmployerDraftJobHistoryCreated.Id = EmployerDraftJobHistory.jobEvent.id

LEFT JOIN (
  SELECT
    jobEvent.creatorId AS userId,
    jobEvent.accountId AS ecomAccountId,
    MIN(jobEvent.createdDate) AS firstCreatedDate
  FROM
    `mstr-sevenseas-prd-afea.dl_employer.employer_draft_job_history` 
  WHERE
    jobEvent.eventType = 'DraftCreatedEvent'
  GROUP BY
    jobEvent.creatorId,
    jobEvent.accountId
) firstAbandoned ON firstAbandoned.userId = EmployerDraftJobHistory.jobEvent.creatorId 
AND firstAbandoned.ecomAccountId = EmployerDraftJobHistory.jobEvent.accountId
AND firstAbandoned.firstCreatedDate = EmployerDraftJobHistoryCreated.firstCreatedDate

LEFT JOIN (
  SELECT
    jobEvent.id AS Id,
    MIN(jobEvent.createdDate) AS firstCreatedDate,
    MAX(jobEvent.modifiedDate) AS lastDeletedDate
  FROM
    `mstr-sevenseas-prd-afea.dl_employer.employer_draft_job_history` 
  WHERE
    jobEvent.eventType = 'DraftDeletedEvent'
  GROUP BY
    id
) EmployerDraftJobHistoryDeleted ON EmployerDraftJobHistoryDeleted.Id = EmployerDraftJobHistory.jobEvent.id

LEFT JOIN (
  SELECT
    jobEvent.id AS Id,
    MAX(jobEvent.modifiedDate) AS lastUpdatedDate
  FROM
    `mstr-sevenseas-prd-afea.dl_employer.employer_draft_job_history`
  WHERE
    jobEvent.eventType = 'DraftUpdatedEvent'
  GROUP BY
    id
) EmployerDraftJobHistoryUpdated ON EmployerDraftJobHistoryUpdated.Id = EmployerDraftJobHistory.jobEvent.id

LEFT JOIN (
  SELECT
    jobEvent.id AS Id,
    MIN(jobEvent.createdDate) AS firstCreatedDate,
    MAX(jobEvent.modifiedDate) AS lastPostedDate
  FROM
    `mstr-sevenseas-prd-afea.dl_employer.employer_draft_job_history`
  WHERE
    jobEvent.eventType = 'DraftPublishedEvent' 
  GROUP BY
    id
) EmployerDraftJobHistoryPublished ON EmployerDraftJobHistoryPublished.Id = EmployerDraftJobHistory.jobEvent.id

LEFT JOIN (
  SELECT
    RatePlan.account_id AS billingAccountId,
    RatePlan.created_by_id AS ratePlanCreatedById,
    RatePlan.updated_date AS ratePlanUpdatedDate,
    Product.created_by_id AS productCreatedById,
    Product.id AS productId,
    Product.name AS currentProductName
  FROM
    `mstr-sevenseas-prd-afea.zuora.rate_plan` RatePlan
  INNER JOIN
    `mstr-sevenseas-prd-afea.zuora.product` Product ON Product.id = RatePlan.product_id
  WHERE
    UPPER(Product.name) IN ('STANDARD','PRO','FREE')
    AND COALESCE(RatePlan.amendment_type,'NewProduct') <> 'RemoveProduct'
    AND RatePlan._fivetran_deleted = FALSE
    AND Product._fivetran_deleted = FALSE
    
  QUALIFY ROW_NUMBER() OVER (PARTITION BY RatePlan.account_id ORDER BY Product.updated_date DESC) = 1
  
) RatePlan ON RatePlan.billingAccountId = EmployerUserAccountCurrent.billingAccountId

WHERE 
  DATE(jobEvent.createdDate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
AND
  DATE(EmployerDraftJobHistoryCreated.firstCreatedDate) != DATE(COALESCE(EmployerDraftJobHistoryDeleted.lastDeletedDate,'1900-01-01'))
AND
  DATE(EmployerDraftJobHistoryCreated.firstCreatedDate) != DATE(COALESCE(EmployerDraftJobHistoryPublished.lastPostedDate,'1900-01-01'))
AND
  DATE(EmployerDraftJobHistoryCreated.firstCreatedDate) != DATE(COALESCE(RatePlan.ratePlanUpdatedDate,'1900-01-01'))
AND
  IFNULL(RatePlan.currentProductName,'') != 'FREE'

QUALIFY ROW_NUMBER() OVER (PARTITION BY jobEvent.accountId,jobEvent.id ORDER BY jobEvent.modifiedDate DESC) = 1

    """
    },
    {"env" : "dev",
    "table" : "RatePlanChargeLastAction", ##### RatePlanChargeLastAction
    "enable" : "false",

    "query" : f"""
WITH extended_1 AS (
  SELECT

    rpc.id AS RatePlanID,
    rpc.account_id AS billingAccountId,
    rpc.subscription_id AS subscriptionId,
    rpc.product_id AS productId,
    rpc.created_date AS ratePlanCreatedDate,
    rpc.updated_date AS ratePlanUpdatedDate,
    product.name AS CurrentPlan,
    product.position_c AS CurrentPlanPosition,

    Subscription.status AS subscriptionStatus,
    TIMESTAMP(Subscription.subscription_start_date) AS subscriptionStartDate,
    TIMESTAMP(Subscription.subscription_end_date) AS subscriptionEndDate,
    Subscription.created_date,

    rpc.effective_start_date CurrentPlanStartDate,
    LAST_VALUE(rpc.effective_end_date) OVER current_plan CurrentPlanEndDate,
    FIRST_VALUE(rpc.effective_start_date) OVER future_plans NextPlanStartDate,
    LAST_VALUE(product.name) OVER past_plans PreviousPlan,
    LAST_VALUE(product.position_c) OVER past_plans PreviousPlanPosition,
    ARRAY_AGG(product.name) OVER past_plans AllPastPlans,
    FIRST_VALUE(product.name) OVER future_plans NextPlan,
    FIRST_VALUE(product.position_c) OVER future_plans NextPlanPosition
  FROM
    `mstr-sevenseas-dev-cd2c.zuora.rate_plan_charge` rpc
  JOIN
    `mstr-sevenseas-dev-cd2c.zuora.product` product ON product.id = rpc.product_id
  JOIN
    `mstr-sevenseas-dev-cd2c.zuora.subscription` Subscription ON Subscription.id = rpc.subscription_id
  WHERE
    rpc._fivetran_deleted IS false
  AND
    product._fivetran_deleted IS false
  AND 
    product.position_c IS NOT NULL -- PAT
  AND
    Subscription._fivetran_deleted IS false

  WINDOW 
    current_plan AS (
      # rate plan charge rows within a plan
      PARTITION BY rpc.account_id, rpc.effective_start_date, product.id
      ORDER BY rpc.charge_number, rpc.Version
      ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
    past_plans AS (
      # all rate plan charge records prior to the current plan range
      # ORDER BY takes one numeric argument only
      PARTITION BY rpc.account_id 
      ORDER BY CAST(CONCAT(UNIX_DATE(rpc.effective_start_date), position_c) AS INT)
      RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
    future_plans AS (
      # all rate plan charge records after the current plan range
      # ORDER BY takes one numeric argument only
      PARTITION BY rpc.account_id 
      ORDER BY CAST(CONCAT(UNIX_DATE(rpc.effective_start_date), position_c) AS INT)
      RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) 
),

# further extend with booleans that describe current, previous, and next plans
extended_2 AS (
  SELECT 
    * EXCEPT(AllPastPlans),
    IFNULL(
      (SELECT DISTINCT true FROM UNNEST(AllPastPlans) x WHERE CONTAINS_SUBSTR(x, "pro"))
      , false) HasPastProPlan,
    IF(LOWER(CurrentPlan) = "pro", true, false) IsCurrentProPlan,
    IF(LOWER(CurrentPlan) = "standard", true, false) IsCurrentStdPlan,
    IF(LOWER(CurrentPlan) = "free", true, false) IsCurrentFreePlan,
    IF(LOWER(PreviousPlan) = "pro", true, false) IsPreviousProPlan,
    IF(LOWER(PreviousPlan) = "standard", true, false) IsPreviousStdPlan,
    IF(PreviousPlan IS null, true, false) IsFirstPlan,
    IF(CurrentPlanPosition < PreviousPlanPosition, true, false) CurrentPlanIsDowngrade,
    IF(CurrentPlanPosition > PreviousPlanPosition, true, false) CurrentPlanIsUpgrade,
    IF(CurrentPlanPosition > NextPlanPosition, true, false) NextPlanIsDowngrade,
    IF(CurrentPlanPosition < NextPlanPosition, true, false) NextPlanIsUpgrade
  FROM 
    extended_1
),

# extend again with pending downgrade/ upgrade dates
extended_3 AS (
  SELECT
    *,
    IF(NextPlanIsDowngrade AND NextPlanStartDate > CURRENT_DATE, NextPlanStartDate, null) PendingDowngradeDate,  
    IF(NextPlanIsUpgrade AND NextPlanStartDate > CURRENT_DATE, NextPlanStartDate, null) PendingUpgradeDate  
  FROM
    extended_2
)

# final query
SELECT 
  RatePlan.*,
  
  MonsterBucks.monsterBucksExpdate,
  MonsterBucks.monsterBucksFreeRemaining,
  MonsterBucks.monsterBucksFree,
  MonsterBucks.monsterBucksRemainingPercentage,
  
  ZuoraOrderDowngrade.lastDowngradeRequestDate,
  ZuoraOrderUpgrade.lastUpgradeRequestDate,
  
  IF (RatePlan.IsFirstPlan = TRUE,'New', IF (RatePlan.CurrentPlanIsUpgrade = TRUE,'Upgrade','Downgrade')) AS lastAction,
  RatePlan.PreviousPlan AS previousProductName,
  RatePlan.NextPlan AS nextProductName,
  IF (RatePlan.NextPlanIsUpgrade = TRUE,'Upgrade', IF (RatePlan.NextPlanIsDowngrade = TRUE,'Downgrade','None')) AS nextAction
  

FROM 
  extended_3 RatePlan
LEFT JOIN (
    SELECT
        accountid_c,
        SAFE_CAST(expdate_c AS DATE) AS monsterBucksExpdate,
        freeremaining_c AS monsterBucksFreeRemaining,
        SAFE_CAST(monsterbucksfree_c AS FLOAT64) AS monsterBucksFree,
        ROUND(SAFE_DIVIDE(freeremaining_c,SAFE_CAST(monsterbucksfree_c AS FLOAT64)) *100,2) AS monsterBucksRemainingPercentage
    FROM
        `mstr-sevenseas-dev-cd2c.zuora.monster_bucks`
    WHERE
        purpose_c = "Subscription Credits"
    AND   
        SAFE_CAST(expdate_c AS DATE) > CURRENT_DATE()
    AND 
        _fivetran_deleted = FALSE 
) MonsterBucks ON MonsterBucks.accountid_c = RatePlan.billingAccountId

  LEFT JOIN
  (
    SELECT
      account_id,
      MAX(created_date) AS lastDowngradeRequestDate,
    FROM
      `mstr-sevenseas-dev-cd2c.zuora.order`
    WHERE
      _fivetran_deleted = FALSE
    AND 
      order_type_c IN ("DowngradeSubscription")
    GROUP BY 
        account_id
  ) ZuoraOrderDowngrade ON ZuoraOrderDowngrade.account_id = RatePlan.billingAccountId

  LEFT JOIN
  (
    SELECT
      account_id,
      MAX(created_date) AS lastUpgradeRequestDate
    FROM
      `mstr-sevenseas-dev-cd2c.zuora.order`
    WHERE
      _fivetran_deleted = FALSE
    AND 
      order_type_c IN ("UpgradeSubscription")
    GROUP BY 
      account_id
  ) ZuoraOrderUpgrade ON ZuoraOrderUpgrade.account_id = RatePlan.billingAccountId

WHERE
    UPPER(RatePlan.CurrentPlan) IN ('STANDARD','PRO','FREE')
AND     
    RatePlan.subscriptionStatus = 'Active'

#AND COALESCE(RatePlan.amendment_type,'NewProduct') <> 'RemoveProduct'

AND 
    RatePlan.CurrentPlanStartDate < current_date()

QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY RatePlan.billingAccountId ORDER BY RatePlan.ratePlanUpdatedDate DESC)

    """
    },
    {"env" : "dev",
    "table" : "SubscriptionUsage", ##### SubscriptionUsage
    "enable" : "false",
    
    "query" : f"""

SELECT
  UserAccount.userId,
  EmployerAccount.ecomAccountId,
  EmployerAccount.billingAccountId,
  
  PostedDate.freeJobFirstPostedDate,
  IF (JobsHistory.jobStatus = 'ACTIVE',TRUE,FALSE) AS freeActiveJob,
  PostedDate.lastFreeMaxApplyReachedDate

FROM

(
  SELECT
    COALESCE(Accounts.accountid, prevPlan.ecomAccountId) AS ecomAccountId,
    User.EmployerUserEvent.user_id AS userId,
    User.EmployerUserEvent.app_metadata.userId as appUserId,
    User.EmployerUserEvent.created_at_epoch_millis AS userCreateDate
  FROM
    `mstr-sevenseas-dev-cd2c.dl_employer.employer_user_events_history` User
  LEFT JOIN
    UNNEST(User.employeruserevent.app_metadata.accounts) AS Accounts
  LEFT JOIN
    (
      SELECT DISTINCT 
        User.EmployerUserEvent.user_id,
        User.EmployerUserEvent.app_metadata.userId as appUserId,
        Accounts.accountid AS ecomAccountId,
        Accounts.role
      FROM
        `mstr-sevenseas-dev-cd2c.dl_employer.employer_user_events_history` User
      INNER JOIN
        UNNEST(User.employeruserevent.app_metadata.accounts) AS Accounts
    ) prevPlan
  ON
    prevPlan.user_id = User.EmployerUserEvent.user_id
  AND
    prevPlan.appUserid = User.EmployerUserEvent.app_metadata.userId
  AND
    Accounts.accountid IS NULL
  WHERE
    User.EmployerUserEvent.email_verified = TRUE

  QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY EmployerUserEvent.user_id,COALESCE(Accounts.accountid, prevPlan.ecomAccountId),User.EmployerUserEvent.app_metadata.userId ORDER BY User.serverEvent.eventTime DESC)

) UserAccount

INNER JOIN (
  SELECT
    accountEvent.id AS ecomAccountId,
    accountEvent.billingAccountId,
    accountEvent.createdByUserId,
    accountEvent.accountName,
    accountEvent.createdTime AS employerAccountCreatedTime
  FROM
    `mstr-sevenseas-dev-cd2c.dl_employer.employer_account_history`

  QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY accountevent.id ORDER BY accountEvent.updatedTime DESC)

) EmployerAccount ON EmployerAccount.ecomAccountId = UserAccount.ecomAccountId

INNER JOIN
  `mstr-sevenseas-dev-cd2c.zuora.account` BillingAccount ON BillingAccount.id = EmployerAccount.billingAccountId

LEFT JOIN (
    SELECT  
        job.accountId AS ecomAccountId,    
        MIN(timestamp(job.jobPosting.datePosted)) AS freeJobFirstPostedDate,
        MAX(job.expiredDate) AS lastFreeMaxApplyReachedDate
    FROM
        `mstr-sevenseas-dev-cd2c.dl_jobs.history` jobsHistory
    INNER JOIN
        UNNEST (job.AttributeValuePairs) entitledSubscriptionPlan ON entitledSubscriptionPlan.name = 'entitledSubscriptionPlan' AND entitledSubscriptionPlan.value = 'FREE'
    GROUP BY
        job.accountId
    
) PostedDate ON PostedDate.ecomAccountId = UserAccount.ecomAccountId

LEFT JOIN (
    SELECT DISTINCT 
        job.accountId AS ecomAccountId,
        creatorId.value AS userId,
        job.jobStatus.status AS jobStatus
    FROM
        `mstr-sevenseas-dev-cd2c.dl_jobs.history` jobsHistory
    INNER JOIN
        UNNEST (job.AttributeValuePairs) creatorId ON creatorId.name = 'creatorId'
    INNER JOIN
        UNNEST (job.AttributeValuePairs) entitledSubscriptionPlan ON entitledSubscriptionPlan.name = 'entitledSubscriptionPlan' AND entitledSubscriptionPlan.value = 'FREE'

    QUALIFY ROW_NUMBER() OVER (PARTITION BY job.jobId ORDER BY job.ModifiedDate DESC) = 1
    
) JobsHistory ON JobsHistory.ecomAccountId = UserAccount.ecomAccountId  AND JobsHistory.jobStatus = 'ACTIVE'

    """
    },
    {"env" : "prd",
    "table" : "SubscriptionUsageFull", ##### SubscriptionUsageFull
    "enable" : "true",
    
    "query" : f"""



WITH cteRatePlan AS (

  SELECT
    RatePlan.AccountID AS billingAccountId,
    RatePlan.SubscriptionID AS subscriptionId,
    RatePlan.ProductID AS productId,
    RatePlan.UpdatedDate AS ratePlanUpdatedDate,
    RatePlan.CurrentPlan AS productName,
    Subscription.SubscriptionStatus AS subscriptionStatus,
    Subscription.SubscriptionStartDate AS subscriptionStartDate,
    Subscription.SubscriptionEndDate AS subscriptionEndDate,
    Subscription.CreatedDate AS subscriptionCreateDate,
    MonsterBucks.monsterBucksExpdate,
    MonsterBucks.monsterBucksFreeRemaining,
    MonsterBucks.monsterBucksFree,
    MonsterBucks.monsterBucksRemainingPercentage,
    ZuoraOrderDowngrade.lastDowngradeRequestDate,
    ZuoraOrderUpgrade.lastUpgradeRequestDate,

    IF (RatePlan.IsFirstPlan = TRUE,'New', IF (RatePlan.CurrentPlanIsUpgrade = TRUE,'Upgrade','Downgrade')) AS lastAction,
    RatePlan.PreviousPlan AS previousProductName,
    RatePlan.NextPlan AS nextProductName,
    IF (RatePlan.NextPlanIsUpgrade = TRUE,'Upgrade', IF (RatePlan.NextPlanIsDowngrade = TRUE,'Downgrade','None')) AS nextAction

  FROM
    `mstr-dwh-dev-83bf.Zuora.RatePlanCharge_extended` RatePlan
  INNER JOIN
    `mstr-dwh-dev-83bf.Zuora.Subscription` Subscription ON Subscription.SubscriptionID = RatePlan.SubscriptionID
  INNER JOIN
    `mstr-sevenseas-dev-cd2c.zuora.rate_plan` rate_plan ON rate_plan.id = RatePlan.RatePlanID
  LEFT JOIN
  (
    SELECT
      accountid_c,
      SAFE_CAST(expdate_c AS DATE) AS monsterBucksExpdate,
      freeremaining_c AS monsterBucksFreeRemaining,
      SAFE_CAST(monsterbucksfree_c AS FLOAT64) AS monsterBucksFree,
      ROUND(SAFE_DIVIDE(freeremaining_c,SAFE_CAST(monsterbucksfree_c AS FLOAT64)) *100,2) AS monsterBucksRemainingPercentage
    FROM
      `mstr-sevenseas-dev-cd2c.zuora.monster_bucks`
    WHERE
      purpose_c = "Subscription Credits"
    AND   
      SAFE_CAST(expdate_c AS DATE) > CURRENT_DATE()
    AND 
      _fivetran_deleted = FALSE 
  ) MonsterBucks ON MonsterBucks.accountid_c = RatePlan.AccountID

  LEFT JOIN
  (
    SELECT
      account_id,
      MAX(created_date) AS lastDowngradeRequestDate,
    FROM
      `mstr-sevenseas-dev-cd2c.zuora.order`
    WHERE
      _fivetran_deleted = FALSE
    AND 
      order_type_c IN ("DowngradeSubscription")
    GROUP BY 
        account_id
  ) ZuoraOrderDowngrade ON ZuoraOrderDowngrade.account_id = RatePlan.AccountID

  LEFT JOIN
  (
    SELECT
      account_id,
      MAX(created_date) AS lastUpgradeRequestDate
    FROM
      `mstr-sevenseas-dev-cd2c.zuora.order`
    WHERE
      _fivetran_deleted = FALSE
    AND 
      order_type_c IN ("UpgradeSubscription")
    GROUP BY 
      account_id
  ) ZuoraOrderUpgrade ON ZuoraOrderUpgrade.account_id = RatePlan.AccountID

  WHERE
    UPPER(RatePlan.CurrentPlan) IN ('STANDARD','PRO','FREE')
  AND 
    Subscription.SubscriptionStatus = 'Active'

    QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY RatePlan.AccountID ORDER BY RatePlan.UpdatedDate DESC)

),

### get min max Campaign Start Dates
dates AS (
  SELECT
    campaignEvent.createdBy,
    TIMESTAMP(MIN(campaignEvent.startDateEpochMillis)) StartDate_min,
    TIMESTAMP(MAX(campaignEvent.startDateEpochMillis)) StartDate_max,
  FROM
    `mstr-sevenseas-dev-cd2c.dl_adtech.campaign_event`
  WHERE
    DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH )
  GROUP BY
    1 
),

### get latest status 
latest AS (
  SELECT
    campaignEvent.createdBy,
    campaignEvent.status
  FROM
    `mstr-sevenseas-dev-cd2c.dl_adtech.campaign_event`
  WHERE
    DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH ) 
  QUALIFY 
    1 = ROW_NUMBER() OVER (PARTITION BY campaignEvent.campaignId ORDER BY campaignEvent.createdEpochMillis DESC ) 
),

### actives only BY user 
actives AS (
  SELECT
    DISTINCT createdBy,
    TRUE has_active_campaign
  FROM
    latest
  WHERE
    LOWER(status) = "active" 
),

### Campaign details joined
campaigns AS (
  SELECT
    dates.createdBy as appUserId,
    dates.StartDate_min as dateFirstVisibleCampaign,
    dates.StartDate_max as dateMostRecentVisibleCampaign,
    actives.has_active_campaign,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), StartDate_min, DAY) days_since_first_campaign,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), StartDate_max, DAY) days_since_last_campaign,
  FROM
    dates
  LEFT JOIN
    actives
  USING
    (createdBy)
)

SELECT
  UserAccount.userId,
  EmployerAccount.ecomAccountId,
  BillingAccount.crm_id AS sfdcAccountId,

  UserAccount.userCreateDate,
  RatePlan.productName,
  RatePlan.subscriptionStatus,
  RatePlan.subscriptionStartDate,
  RatePlan.ratePlanUpdatedDate,

  c.dateFirstVisibleCampaign,
  DATE_DIFF(CURRENT_DATE(),DATE(c.dateFirstVisibleCampaign), DAY) AS daysSinceFirstVisibleCampaign,
  c.has_active_campaign AS activeCampaignExists,
  c.dateMostRecentVisibleCampaign,
  DATE_DIFF(CURRENT_DATE(),DATE(c.dateMostRecentVisibleCampaign), DAY) AS daysSinceMostRecentVisibleCampaign,

  BillableUsage.billableEventType,
  BillableUsage.dateFirstBillableEvent,
  BillableUsage.daysSinceFirstBillableEvent,
  BillableUsage.dateMostRecentBillableEvent,
  BillableUsage.daysSinceMostRecentBillableEvent,

  RatePlan.monsterBucksExpdate,
  RatePlan.monsterBucksFree,
  RatePlan.monsterBucksFreeRemaining,
  RatePlan.monsterBucksRemainingPercentage,
  RatePlan.lastDowngradeRequestDate,
  RatePlan.lastUpgradeRequestDate,

  SubscriptionHistory.currentProductEndDate,
  SubscriptionHistory.daysRemainingUntilEndDate,

  RatePlan.lastAction,
  RatePlan.previousProductName,
  RatePlan.nextProductName,
  RatePlan.nextAction,

  PostedDate.freeJobFirstPostedDate,
  IF (JobsHistory.jobStatus = 'ACTIVE',TRUE,FALSE) AS freeActiveJob,
  PostedDate.lastFreeMaxApplyReachedDate

  #TIMESTAMP('{{var.value.v_sfmc_b2b_mart_etldate}}') as BQETLDate

FROM

(
  SELECT
    COALESCE(Accounts.accountid, prevPlan.ecomAccountId) AS ecomAccountId,
    User.EmployerUserEvent.user_id AS userId,
    User.EmployerUserEvent.app_metadata.userId as appUserId,
    User.EmployerUserEvent.created_at_epoch_millis AS userCreateDate
  FROM
    `mstr-sevenseas-dev-cd2c.dl_employer.employer_user_events_history` User
  LEFT JOIN
    UNNEST(User.employeruserevent.app_metadata.accounts) AS Accounts
  LEFT JOIN
    (
      SELECT DISTINCT 
        User.EmployerUserEvent.user_id,
        User.EmployerUserEvent.app_metadata.userId as appUserId,
        Accounts.accountid AS ecomAccountId,
        Accounts.role
      FROM
        `mstr-sevenseas-dev-cd2c.dl_employer.employer_user_events_history` User
      INNER JOIN
        UNNEST(User.employeruserevent.app_metadata.accounts) AS Accounts
    ) prevPlan
  ON
    prevPlan.user_id = User.EmployerUserEvent.user_id
  AND
    prevPlan.appUserid = User.EmployerUserEvent.app_metadata.userId
  AND
    Accounts.accountid IS NULL
  WHERE
    User.EmployerUserEvent.email_verified = TRUE

  QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY EmployerUserEvent.user_id,COALESCE(Accounts.accountid, prevPlan.ecomAccountId),User.EmployerUserEvent.app_metadata.userId ORDER BY User.serverEvent.eventTime DESC)

) UserAccount

INNER JOIN (
  SELECT
    accountEvent.id AS ecomAccountId,
    accountEvent.billingAccountId,
    accountEvent.createdByUserId,
    accountEvent.accountName,
    accountEvent.createdTime AS employerAccountCreatedTime
  FROM
    `mstr-sevenseas-dev-cd2c.dl_employer.employer_account_history`

  QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY accountevent.id ORDER BY accountEvent.updatedTime DESC)

) EmployerAccount ON EmployerAccount.ecomAccountId = UserAccount.ecomAccountId

INNER JOIN
  `mstr-sevenseas-dev-cd2c.zuora.account` BillingAccount ON BillingAccount.id = EmployerAccount.billingAccountId

LEFT JOIN
  cteRatePlan RatePlan ON RatePlan.billingAccountId = EmployerAccount.billingAccountId

LEFT JOIN
  campaigns c ON c.appUserId = UserAccount.appUserId

LEFT JOIN (
  SELECT
    billableUsageEvent.accountId AS ecomAccountId,
    c.id as appUserId,
    billableUsageEvent.eventType AS billableEventType,
    MIN(TIMESTAMP_MILLIS(serverEvent.eventTime)) AS dateFirstBillableEvent,
    MAX(TIMESTAMP_MILLIS(serverEvent.eventTime)) AS dateMostRecentBillableEvent,
    DATE_DIFF(CURRENT_DATE(),DATE(MIN(TIMESTAMP_MILLIS(serverEvent.eventTime))), DAY) AS daysSinceFirstBillableEvent,
    DATE_DIFF(CURRENT_DATE(),DATE(MAX(TIMESTAMP_MILLIS(serverEvent.eventTime))), DAY) AS daysSinceMostRecentBillableEvent
  FROM
    `mstr-sevenseas-dev-cd2c.dl_employer.billable_usage_events` BillableUsageEvents
  LEFT JOIN
    unnest(billableUsageEvent.usages) u
  LEFT JOIN
    unnest(u.correlations) c
  WHERE
    billableUsageEvent.eventType = "CandidateSearchEvent"
  AND
    c.kind = 'userId'
  GROUP BY
    billableUsageEvent.accountId,
    c.id,
    billableUsageEvent.eventType

) BillableUsage ON BillableUsage.ecomAccountId = UserAccount.ecomAccountId AND BillableUsage.appUserId = UserAccount.appUserId

LEFT JOIN (
  SELECT
    subscriptionEvent.ecommAccountId,
    subscriptionEvent.billingAccountId,
    subscriptionEvent.currentProduct.effectiveEndDate AS currentProductEndDate,
    DATE_DIFF(COALESCE(DATE(subscriptionEvent.currentProduct.effectiveEndDate),DATE(subscriptionEvent.nextProduct.effectiveStartDate)), CURRENT_DATE(), DAY) AS daysRemainingUntilEndDate
  FROM
    `mstr-sevenseas-dev-cd2c.dl_employer.employer_subscription_history` SubscriptionHistory
  WHERE
    (subscriptionEvent.currentProduct.effectiveEndDate > CURRENT_TIMESTAMP()
      OR subscriptionEvent.nextProduct.effectiveStartDate > CURRENT_TIMESTAMP())

  QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY subscriptionEvent.ecommAccountId ORDER BY serverEvent.eventTime DESC)

) SubscriptionHistory ON SubscriptionHistory.ecommAccountId = UserAccount.ecomAccountId

LEFT JOIN (
    SELECT  
        job.accountId AS ecomAccountId,    
        MIN(job.jobPosting.datePosted) AS freeJobFirstPostedDate,
        MAX(job.expiredDate) AS lastFreeMaxApplyReachedDate
    FROM
        `mstr-sevenseas-dev-cd2c.dl_jobs.history` jobsHistory
    INNER JOIN
        UNNEST (job.AttributeValuePairs) entitledSubscriptionPlan ON entitledSubscriptionPlan.name = 'entitledSubscriptionPlan' AND entitledSubscriptionPlan.value = 'FREE'
    GROUP BY
        job.accountId
    
) PostedDate ON PostedDate.ecomAccountId = UserAccount.ecomAccountId

LEFT JOIN (
    SELECT DISTINCT 
        job.accountId AS ecomAccountId,
        creatorId.value AS userId,
        job.jobStatus.status AS jobStatus
    FROM
        `mstr-sevenseas-dev-cd2c.dl_jobs.history` jobsHistory
    INNER JOIN
        UNNEST (job.AttributeValuePairs) creatorId ON creatorId.name = 'creatorId'
    INNER JOIN
        UNNEST (job.AttributeValuePairs) entitledSubscriptionPlan ON entitledSubscriptionPlan.name = 'entitledSubscriptionPlan' AND entitledSubscriptionPlan.value = 'FREE'

    QUALIFY ROW_NUMBER() OVER (PARTITION BY job.jobId ORDER BY job.ModifiedDate DESC) = 1
    
) JobsHistory ON JobsHistory.ecomAccountId = UserAccount.ecomAccountId  AND JobsHistory.jobStatus = 'ACTIVE'

ORDER BY
  UserAccount.userCreateDate DESC

    """
    }
    
]

    for qdl in query_dict_list:

        if qdl["enable"] == "true":
        
          print(f'Running - {qdl["table"]} - {datetime.now()}')
        
          # Run the query and save results to CSV
          csv_file_path = run_query_and_save_to_csv(qdl["query"], qdl["env"], qdl["table"])
          print(f'{qdl["env"]} {qdl["table"]} CSV file saved to: {csv_file_path}')
          
          print(f'Finished - {qdl["table"]} - {datetime.now()}')


if __name__ == "__main__":
    main()