------------------------------------------- UNIVERSO DE CLIENTE -------------------------------------------------------

--- Comando que dirige las consultas a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

ALTER TABLE MIS_DM_BALANCE_RESULT_CLI
DROP IF EXISTS PARTITION (DATA_DATE = '${var:periodo}');

insert into mis_dm_balance_result_cli
partition (data_date)
select a.cod_entity, a.cod_blce_prod, a.idf_cli, a.typ_doc_id, a.nom_name, a.nom_lastn, cast(a.eopbal_tot as decimal(30,10)) as eopbal_tot, cast(a.eopbal_cap as decimal(30,10)) as eopbal_cap, 
cast(a.eopbal_tot_rpt as decimal(30,10)) as eopbal_tot_rpt, cast(a.eopbal_tot_usd as decimal(30,10)) as eopbal_tot_usd, cast(a.eopbal_tot_reg as decimal(30,10)) as eopbal_tot_reg,
cast(a.eopbal_tot - isnull(b.eopbal_tot,0) as decimal(30,10)) as eopbal_tot_var_day, cast(a.eopbal_tot - isnull(c.eopbal_tot,0) as decimal(30,10)) as eopbal_tot_var_last_month, 
cast(a.eopbal_tot - isnull(d.eopbal_tot,0) as decimal(30,10)) as eopbal_tot_var_month, cast(a.eopbal_tot - isnull(e.eopbal_tot,0) as decimal(30,10)) as eopbal_tot_var_last_year,
cast(a.eopbal_tot - isnull(f.eopbal_tot,0) as decimal(30,10)) as eopbal_tot_var_year,
cast(a.eopbal_cap - isnull(b.eopbal_cap,0) as decimal(30,10)) as eopbal_cap_var_day, cast(a.eopbal_cap - isnull(c.eopbal_cap,0) as decimal(30,10)) as eopbal_cap_var_last_month, 
cast(a.eopbal_cap - isnull(d.eopbal_cap,0) as decimal(30,10)) as eopbal_cap_var_month, cast(a.eopbal_cap - isnull(e.eopbal_cap,0) as decimal(30,10)) as eopbal_cap_var_last_year,
cast(a.eopbal_cap - isnull(f.eopbal_cap,0) as decimal(30,10)) as eopbal_cap_var_year,
cast(a.eopbal_tot_rpt - isnull(b.eopbal_tot_rpt,0) as decimal(30,10)) as eopbal_tot_rpt_var_day, cast(a.eopbal_tot_rpt - isnull(c.eopbal_tot_rpt,0) as decimal(30,10)) as eopbal_tot_rpt_var_last_month, 
cast(a.eopbal_tot_rpt - isnull(d.eopbal_tot_rpt,0) as decimal(30,10)) as eopbal_tot_rpt_var_month, cast(a.eopbal_tot_rpt - isnull(e.eopbal_tot_rpt,0) as decimal(30,10)) as eopbal_tot_rpt_var_last_year,
cast(a.eopbal_tot_rpt - isnull(f.eopbal_tot_rpt,0) as decimal(30,10)) as eopbal_tot_rpt_var_year,
cast(a.eopbal_tot_usd - isnull(b.eopbal_tot_usd,0) as decimal(30,10)) as eopbal_tot_usd_var_day, cast(a.eopbal_tot_usd - isnull(c.eopbal_tot_usd,0) as decimal(30,10)) as eopbal_tot_usd_var_last_month, 
cast(a.eopbal_tot_usd - isnull(d.eopbal_tot_usd,0) as decimal(30,10)) as eopbal_tot_usd_var_month, cast(a.eopbal_tot_usd - isnull(e.eopbal_tot_usd,0) as decimal(30,10)) as eopbal_tot_usd_var_last_year,
cast(a.eopbal_tot_usd - isnull(f.eopbal_tot_usd,0) as decimal(30,10)) as eopbal_tot_usd_var_year,
cast(a.eopbal_tot_reg - isnull(b.eopbal_tot_reg,0) as decimal(30,10)) as eopbal_tot_reg_var_day, cast(a.eopbal_tot_reg - isnull(c.eopbal_tot_reg,0) as decimal(30,10)) as eopbal_tot_reg_var_last_month, 
cast(a.eopbal_tot_reg - isnull(d.eopbal_tot_reg,0) as decimal(30,10)) as eopbal_tot_reg_var_month, cast(a.eopbal_tot_reg - isnull(e.eopbal_tot_reg,0) as decimal(30,10)) as eopbal_tot_reg_var_last_year,
cast(a.eopbal_tot_reg - isnull(f.eopbal_tot_reg,0) as decimal(30,10)) as eopbal_tot_reg_var_year,
a.data_date
from (select data_date, cod_entity, cod_blce_prod, idf_cli, typ_doc_id, nom_name, nom_lastn, sum(eopbal_tot) as eopbal_tot, sum(eopbal_cap) as eopbal_cap, sum(eopbal_tot_rpt) as eopbal_tot_rpt,
        sum(eopbal_tot_usd) as eopbal_tot_usd, sum(eopbal_tot_reg) as eopbal_tot_reg
        from mis_dm_balance_result
        where data_date = '${var:periodo}' and ind_pool is null and cod_value = 'CAP'
        group by data_date, cod_entity, cod_blce_prod, idf_cli, typ_doc_id, nom_name, nom_lastn) a
left join (select cod_entity, cod_blce_prod, idf_cli, typ_doc_id, nom_name, nom_lastn, sum(eopbal_tot) as eopbal_tot, sum(eopbal_cap) as eopbal_cap, sum(eopbal_tot_rpt) as eopbal_tot_rpt,
        sum(eopbal_tot_usd) as eopbal_tot_usd, sum(eopbal_tot_reg) as eopbal_tot_reg
        from mis_dm_balance_result 
        where TO_TIMESTAMP(data_date, 'yyyyMMdd') = subdate(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'),1) and ind_pool is null and cod_value = 'CAP'
        group by cod_entity, cod_blce_prod, idf_cli, typ_doc_id, nom_name, nom_lastn) b
on a.cod_entity = b.cod_entity and a.cod_blce_prod = b.cod_blce_prod and a.idf_cli = b.idf_cli and isnull(a.typ_doc_id,'') = isnull(b.typ_doc_id,'') and isnull(a.nom_name,'') = isnull(b.nom_name,'') and isnull(a.nom_lastn,'') = isnull(b.nom_lastn,'')
left join (select cod_entity, cod_blce_prod, idf_cli, typ_doc_id, nom_name, nom_lastn, sum(eopbal_tot) as eopbal_tot, sum(eopbal_cap) as eopbal_cap, sum(eopbal_tot_rpt) as eopbal_tot_rpt,
        sum(eopbal_tot_usd) as eopbal_tot_usd, sum(eopbal_tot_reg) as eopbal_tot_reg 
        from mis_dm_balance_result 
        where TO_TIMESTAMP(data_date, 'yyyyMMdd') = DATE_SUB(TRUNC(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), 'MM'),1) and ind_pool is null and cod_value = 'CAP'
        group by cod_entity, cod_blce_prod, idf_cli, typ_doc_id, nom_name, nom_lastn) c
on a.cod_entity = c.cod_entity and a.cod_blce_prod = c.cod_blce_prod and a.idf_cli = c.idf_cli and isnull(a.typ_doc_id,'') = isnull(c.typ_doc_id,'') and isnull(a.nom_name,'') = isnull(c.nom_name,'') and isnull(a.nom_lastn,'') = isnull(c.nom_lastn,'')
left join (select cod_entity, cod_blce_prod, idf_cli, typ_doc_id, nom_name, nom_lastn, sum(eopbal_tot) as eopbal_tot, sum(eopbal_cap) as eopbal_cap, sum(eopbal_tot_rpt) as eopbal_tot_rpt,
        sum(eopbal_tot_usd) as eopbal_tot_usd, sum(eopbal_tot_reg) as eopbal_tot_reg 
        from mis_dm_balance_result 
        where TO_TIMESTAMP(data_date, 'yyyyMMdd') = CASE WHEN DAY(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd')) > DAY(LAST_DAY(ADD_MONTHS(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), -1))) THEN 
        LAST_DAY(ADD_MONTHS(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), -1))
    ELSE
        DATE_ADD(TO_TIMESTAMP(CONCAT(FROM_TIMESTAMP(MONTHS_SUB(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'),1),'yyyyMM'),'01'), 'yyyyMMdd'),DAY(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'))-1)
        END
        and ind_pool is null and cod_value = 'CAP'
        group by cod_entity, cod_blce_prod, idf_cli, typ_doc_id, nom_name, nom_lastn) d
on a.cod_entity = d.cod_entity and a.cod_blce_prod = d.cod_blce_prod and a.idf_cli = d.idf_cli and isnull(a.typ_doc_id,'') = isnull(d.typ_doc_id,'') and isnull(a.nom_name,'') = isnull(d.nom_name,'') and isnull(a.nom_lastn,'') = isnull(d.nom_lastn,'')
left join (select cod_entity, cod_blce_prod, idf_cli, typ_doc_id, nom_name, nom_lastn, sum(eopbal_tot) as eopbal_tot, sum(eopbal_cap) as eopbal_cap, sum(eopbal_tot_rpt) as eopbal_tot_rpt,
        sum(eopbal_tot_usd) as eopbal_tot_usd, sum(eopbal_tot_reg) as eopbal_tot_reg 
        from mis_dm_balance_result 
        where TO_TIMESTAMP(data_date, 'yyyyMMdd') = DATE_SUB(TRUNC(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'), 'YY'),1) and ind_pool is null and cod_value = 'CAP'
        group by cod_entity, cod_blce_prod, idf_cli, typ_doc_id, nom_name, nom_lastn) e
on a.cod_entity = e.cod_entity and a.cod_blce_prod = e.cod_blce_prod and a.idf_cli = e.idf_cli and isnull(a.typ_doc_id,'') = isnull(e.typ_doc_id,'') and isnull(a.nom_name,'') = isnull(e.nom_name,'') and isnull(a.nom_lastn,'') = isnull(e.nom_lastn,'')
left join (select cod_entity, cod_blce_prod, idf_cli, typ_doc_id, nom_name, nom_lastn, sum(eopbal_tot) as eopbal_tot, sum(eopbal_cap) as eopbal_cap, sum(eopbal_tot_rpt) as eopbal_tot_rpt,
        sum(eopbal_tot_usd) as eopbal_tot_usd, sum(eopbal_tot_reg) as eopbal_tot_reg 
        from mis_dm_balance_result 
        where TO_TIMESTAMP(data_date, 'yyyyMMdd') = years_sub(TO_TIMESTAMP('${var:periodo}', 'yyyyMMdd'),1) and ind_pool is null and cod_value = 'CAP'
        group by cod_entity, cod_blce_prod, idf_cli, typ_doc_id, nom_name, nom_lastn) f
on a.cod_entity = f.cod_entity and a.cod_blce_prod = f.cod_blce_prod and a.idf_cli = f.idf_cli and isnull(a.typ_doc_id,'') = isnull(f.typ_doc_id,'') and isnull(a.nom_name,'') = isnull(f.nom_name,'') and isnull(a.nom_lastn,'') = isnull(f.nom_lastn,'')
;