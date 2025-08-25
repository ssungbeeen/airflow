from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

def execute_mysql_queries_1():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    queries = queries = [
        """DELETE FROM jac_usr_mst WHERE usr_id IN (
               SELECT T1.USER_ID
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T2.USER_CODE IS NULL
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y');""",
        """DELETE FROM jac_usr_grp WHERE usr_id IN (
               SELECT T1.USER_ID
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T2.USER_CODE IS NULL
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y');""",
        """DELETE FROM jac_usr_product WHERE usr_id IN (
               SELECT T1.USER_ID
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T2.USER_CODE IS NULL
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y');""",
        """INSERT INTO jac_usr_mst
               SELECT T1.USER_ID, T1.USER_NM, NULL, DATE_FORMAT(NOW(), "%Y-%m-%d"), 0, 'ko', T1.PART_CODE, NULL, NOW(), 'Y', 'SYSTEM', NOW(), 'SYSTEM', NOW()
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T2.USER_CODE IS NULL
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y';""",
        """INSERT INTO jac_usr_product
               SELECT 'dashboard', T1.USER_ID, 'SYSTEM', NOW()
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T2.USER_CODE IS NULL
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y';""",
        """INSERT INTO jac_usr_grp
               SELECT '9001', T1.USER_ID, 'SYSTEM', NOW()
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T2.USER_CODE IS NULL
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y';""",
        """INSERT INTO jac_usr_grp
               SELECT CASE WHEN T1.PART_CODE IN ('1','3','4','5','6','7','8','9','26','27','39','40') THEN '9002'
                           WHEN T1.PART_CODE  = '38' THEN '9003'
                           WHEN T1.PART_CODE  = '12' THEN '9004'
                           WHEN T1.PART_CODE  = '10' THEN '9005'
                      END AS GROUP_CODE, T1.USER_ID, 'SYSTEM', NOW()
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T2.USER_CODE IS NULL
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y'
               AND T1.PART_CODE IN ('1','3','4','5','6','7','8','9','26','27','39','40', '38', '12', '10');""",
        """INSERT INTO jac_usr_grp
           SELECT T1.PART_CODE, T1.USER_ID,'SYSTEM', NOW()
             FROM (SELECT T1.PART_CODE, T1.USER_ID
                     FROM TLS_USER T1
                     LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                    WHERE T2.USER_ID IS NULL
                      AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                      AND T1.BI_USE_YN = 'Y'
                    UNION ALL
                   SELECT T1.group_code AS PART_CODE, T2.USER_ID FROM jac_group_mst T1
                     LEFT JOIN (SELECT T1.PART_CODE, T1.USER_ID
                                  FROM TLS_USER T1
                                  LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                                 WHERE T2.USER_ID IS NULL
                                   AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                                   AND T1.BI_USE_YN = 'Y'
                               ) T2 ON T1.GROUP_CODE != T2.PART_CODE
                    WHERE T1.group_desc like "%,data" and T1.group_code != 'T'
                    ORDER BY CAST(PART_CODE AS UNSIGNED)
                  ) T1
            WHERE T1.USER_ID IS NOT NULL;"""
        ,
        """INSERT INTO jac_usr_grp
               SELECT 'T', T1.USER_ID, 'SYSTEM', NOW()
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T2.USER_CODE IS NULL
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y';"""
    ]
    
    for query in queries:
        mysql_hook.run(query)


def execute_mysql_queries_2():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    
    queries = queries = [
        """DELETE FROM jac_usr_mst WHERE usr_id IN (
               SELECT T1.USER_ID
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T1.BI_USE_YN != T2.BI_USE_YN
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y');""",
        """DELETE FROM jac_usr_grp WHERE usr_id IN (
               SELECT T1.USER_ID
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T1.BI_USE_YN != T2.BI_USE_YN
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y');""",
        """DELETE FROM jac_usr_product WHERE usr_id IN (
               SELECT T1.USER_ID
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T1.BI_USE_YN != T2.BI_USE_YN
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y');""",
        """INSERT INTO jac_usr_mst
               SELECT T1.USER_ID, T1.USER_NM, NULL, DATE_FORMAT(NOW(), "%Y-%m-%d"), 0, 'ko', T1.PART_CODE, NULL, NOW(), 'Y', 'SYSTEM', NOW(), 'SYSTEM', NOW()
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T1.BI_USE_YN != T2.BI_USE_YN
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y';""",
        """INSERT INTO jac_usr_product
               SELECT 'dashboard', T1.USER_ID, 'SYSTEM', NOW()
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T1.BI_USE_YN != T2.BI_USE_YN
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y';""",
        """INSERT INTO jac_usr_grp
               SELECT '9001', T1.USER_ID, 'SYSTEM', NOW()
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T1.BI_USE_YN != T2.BI_USE_YN
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y';""",
        """INSERT INTO jac_usr_grp
               SELECT CASE WHEN T1.PART_CODE IN ('1','3','4','5','6','7','8','9','26','27','39','40') THEN '9002'
                           WHEN T1.PART_CODE  = '38' THEN '9003'
                           WHEN T1.PART_CODE  = '12' THEN '9004'
                           WHEN T1.PART_CODE  = '10' THEN '9005'
                      END AS GROUP_CODE, T1.USER_ID, 'SYSTEM', NOW()
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T1.BI_USE_YN != T2.BI_USE_YN
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y'
               AND T1.PART_CODE IN ('1','3','4','5','6','7','8','9','26','27','39','40', '38', '12', '10');""",
        """INSERT INTO jac_usr_grp
           SELECT T1.PART_CODE, T1.USER_ID,'SYSTEM', NOW()
             FROM (SELECT T1.PART_CODE, T1.USER_ID
                     FROM TLS_USER T1
                     LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                    WHERE T1.BI_USE_YN != T2.BI_USE_YN
                      AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                      AND T1.BI_USE_YN = 'Y'
                    UNION ALL
                   SELECT T1.group_code AS PART_CODE, T2.USER_ID FROM jac_group_mst T1
                     LEFT JOIN (SELECT T1.PART_CODE, T1.USER_ID
                                  FROM TLS_USER T1
                                  LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                                WHERE T1.BI_USE_YN != T2.BI_USE_YN
                                   AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                                   AND T1.BI_USE_YN = 'Y'
                               ) T2 ON T1.GROUP_CODE != T2.PART_CODE
                    WHERE T1.group_desc like "%,data" and T1.group_code != 'T'
                    ORDER BY CAST(PART_CODE AS UNSIGNED)
                  ) T1
            WHERE T1.USER_ID IS NOT NULL;"""
        ,
        """INSERT INTO jac_usr_grp
               SELECT 'T', T1.USER_ID, 'SYSTEM', NOW()
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T1.BI_USE_YN != T2.BI_USE_YN
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y';"""
    ]
    
    for query in queries:
        mysql_hook.run(query)    
        
        
        

def execute_mysql_queries_3():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    queries = ["""INSERT INTO user_action_log
               SELECT T1.USER_ID, "신규", now()
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T2.USER_CODE IS NULL
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y';
               COMMIT;""",

               """INSERT INTO user_action_log
               SELECT T1.USER_ID, "신규", now()
                 FROM TLS_USER T1
                 LEFT JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID
                WHERE T1.BI_USE_YN != T2.BI_USE_YN
                  AND (T2.BI_USE_YN = 'N' OR T2.BI_USE_YN IS NULL)
                  AND T1.BI_USE_YN = 'Y';
               COMMIT;""",

               """INSERT INTO user_action_log SELECT T1.USER_ID, '부서이동', now() FROM TLS_USER T1
               INNER JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID AND T1.PART_CODE != T2.PART_CODE AND T2.BI_USE_YN = 'Y'
               WHERE T1.BI_USE_YN = 'Y';""",
               """DELETE FROM jac_usr_grp WHERE usr_id IN (SELECT T1.USER_ID FROM TLS_USER T1
               INNER JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID AND T1.PART_CODE != T2.PART_CODE AND T2.BI_USE_YN = 'Y'
               WHERE T1.BI_USE_YN = 'Y') AND GROUP_CODE != '9001'
               AND GROUP_CODE NOT IN (SELECT GROUP_CODE FROM jac_group_mst WHERE group_desc LIKE '%,data');""",
               """UPDATE jac_usr_mst t1 JOIN (SELECT T1.USER_ID, T1.PART_CODE FROM TLS_USER T1
               INNER JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID AND T1.PART_CODE != T2.PART_CODE AND T2.BI_USE_YN = 'Y' WHERE T1.BI_USE_YN = 'Y') t2 ON t1.usr_id = t2.USER_ID
               SET t1.usr_desc = t2.PART_CODE;""",
               """INSERT INTO jac_usr_grp
               SELECT CASE WHEN T1.PART_CODE IN ('1','3','4','5','6','7','8','9','26','27','39','40') THEN '9002'
               WHEN T1.PART_CODE  = '38' THEN '9003'
               WHEN T1.PART_CODE  = '12' THEN '9004'
               WHEN T1.PART_CODE  = '10' THEN '9005'
               END AS GROUP_CODE, T1.USER_ID, 'SYSTEM', NOW() FROM TLS_USER T1
               INNER JOIN TLS_USER_TEMP T2 ON T1.USER_ID = T2.USER_ID AND T1.PART_CODE != T2.PART_CODE AND T2.BI_USE_YN = 'Y'
               WHERE T1.BI_USE_YN = 'Y' AND T1.PART_CODE IN ('1','3','4','5','6','7','8','9','26','27','39','40', '38', '12', '10');
               """          
    ]
    for query in queries:
        mysql_hook.run(query)           
   
   




def execute_mysql_queries_4():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    queries = ["""INSERT INTO user_action_log SELECT tut.USER_ID, '삭제', now() FROM TLS_USER_TEMP tut
               LEFT JOIN TLS_USER tu ON tut.USER_ID = tu.USER_ID AND tu.BI_USE_YN = 'Y'
               WHERE tut.BI_USE_YN = 'Y' AND tu.USER_ID IS NULL;""",
               """DELETE FROM jac_usr_mst jumi WHERE usr_id IN (SELECT tut.USER_ID FROM TLS_USER_TEMP tut
               LEFT JOIN TLS_USER tu ON tut.USER_ID = tu.USER_ID AND tu.BI_USE_YN = 'Y' WHERE tut.BI_USE_YN = 'Y'
               AND tu.USER_ID IS NULL);""",
               """DELETE FROM jac_usr_grp WHERE usr_id IN (SELECT tut.USER_ID FROM TLS_USER_TEMP tut
               LEFT JOIN TLS_USER tu ON tut.USER_ID = tu.USER_ID AND tu.BI_USE_YN = 'Y' WHERE tut.BI_USE_YN = 'Y'
               AND tu.USER_ID IS NULL);""",
               """DELETE FROM jac_usr_product
               WHERE usr_id IN (SELECT tut.USER_ID FROM TLS_USER_TEMP tut
               LEFT JOIN TLS_USER tu ON tut.USER_ID = tu.USER_ID AND tu.BI_USE_YN = 'Y'
               WHERE tut.BI_USE_YN = 'Y' AND tu.USER_ID IS NULL);"""

    ]
    for query in queries:
        mysql_hook.run(query)   
   
   
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

         
dag= DAG(
    dag_id='Permission_Pipeline',
    schedule_interval='30 6 * * 1-7',
    start_date=pendulum.datetime(2025, 3, 30, tz="Asia/Seoul"),
    catchup=False,
    tags=['mysql', 'etl'])


new_employee_pipeline = PythonOperator(
        task_id='new_employee_pipeline',
        python_callable=execute_mysql_queries_1,
        dag=dag
    )

new_employee_pipeline_case_2 = PythonOperator(
        task_id='new_employee_pipeline_case_2',
        python_callable=execute_mysql_queries_2,
        dag=dag
    ) 
    
transferred_employee_pipeline = PythonOperator(
        task_id='transferred_employee_pipeline',
        python_callable=execute_mysql_queries_3,
        dag=dag
    )  
 
resigned_employee_pipeline = PythonOperator(
        task_id='resigned_employee_pipeline',
        python_callable=execute_mysql_queries_4,
        dag=dag
    )      
    
new_employee_pipeline >> new_employee_pipeline_case_2 >> transferred_employee_pipeline >> resigned_employee_pipeline
