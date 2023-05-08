---------------------------------------------------------- CONTRAVALORACIÓN Y CARGA FINAL -------------------------------------------

--- Comando que dirige las consultas a la base de datos apropiada ---
USE ${var:base_datos};
SET DECIMAL_V2=FALSE;

TRUNCATE TABLE MIS_VAL_CLOSE_DUP;

insert into mis_val_close_dup
select 'MIS_PAR_REL_CAF_OPER', cod_entity, cod_product, cod_subproduct, cod_currency, cod_blce_status, cod_value, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select cod_entity, cod_product, cod_subproduct, cod_currency, cod_blce_status, cod_value, count(*) as num from mis_par_rel_caf_oper
group by cod_entity, cod_product, cod_subproduct, cod_currency, cod_blce_status, cod_value
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_REL_CAF_ACC', cod_entity, 'N/A', 'N/A', cod_currency, 'N/A', 'N/A', cod_gl, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select cod_entity, cod_currency, cod_gl, count(*) as num from mis_par_rel_caf_acc
group by cod_entity, cod_currency, cod_gl
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_REL_BP_ACC', cod_entity, 'N/A', 'N/A', cod_currency, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cod_gl_group, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select cod_entity, cod_currency, cod_gl_group, count(*) as num from MIS_PAR_REL_BP_ACC
group by cod_entity, cod_currency, cod_gl_group
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_REL_BP_OPER', cod_entity, cod_product, cod_subproduct, cod_currency, 'N/A', 'N/A', 'N/A', cod_act_type, cod_rate_type, 'N/A', cod_gl_group, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select cod_entity, cod_product, cod_subproduct, cod_currency, cod_act_type, cod_rate_type, cod_gl_group, count(*) as num from MIS_PAR_REL_BP_OPER
group by cod_entity, cod_product, cod_subproduct, cod_currency, cod_act_type, cod_rate_type, cod_gl_group
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_REL_PL_ACC', cod_entity, 'N/A', 'N/A', cod_currency, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cod_gl_group, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select cod_entity, cod_currency, cod_gl_group, count(*) as num from mis_par_rel_pl_acc
group by cod_entity, cod_currency, cod_gl_group
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_REL_BL_ACC', cod_entity, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cod_gl_group, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select cod_entity, cod_gl_group, count(*) as num from mis_par_rel_bl_acc
group by cod_entity, cod_gl_group
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_REL_BL_OPER', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cod_typ_clnt, 'N/A', cod_blce_prod, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select cod_typ_clnt, cod_blce_prod, count(*) as num from mis_par_rel_bl_oper
group by cod_typ_clnt, cod_blce_prod
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_ALLOC_AC_DRI', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cod_driver, cod_acco_cent, cod_expense, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select cod_driver, cod_acco_cent, cod_expense, count(*) as num from MIS_PAR_ALLOC_AC_DRI
group by cod_driver, cod_acco_cent, cod_expense
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_ALLOC_AC_ENG', cod_entity, 'N/A', 'N/A', cod_currency, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cod_gl_group, 'N/A', 'N/A', cod_acco_cent, cod_expense, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select cod_entity, cod_gl_group, cod_acco_cent, cod_expense, cod_currency, count(*) as num from MIS_PAR_ALLOC_AC_ENG
group by cod_entity, cod_gl_group, cod_acco_cent, cod_expense, cod_currency
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_ALLOC_SEG_DRI', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cod_blce_prod, cod_driver, 'N/A', 'N/A', cod_segment, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select cod_driver, cod_segment, cod_blce_prod, count(*) as num from MIS_PAR_ALLOC_SEG_DRI
group by cod_driver, cod_segment, cod_blce_prod
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_ALLOC_SEG_ENG', cod_entity, 'N/A', 'N/A', cod_currency, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cod_gl_group, 'N/A', 'N/A', cod_acco_cent, cod_expense, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select cod_entity, cod_gl_group, cod_acco_cent, cod_expense, cod_currency, count(*) as num from MIS_PAR_ALLOC_SEG_ENG
group by cod_entity, cod_gl_group, cod_acco_cent, cod_expense, cod_currency
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_REL_EXP_TYP', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cod_gl_group, 'N/A', 'N/A', cod_acco_cent, cod_expense, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select cod_gl_group, cod_acco_cent, cod_expense, count(*) as num from MIS_PAR_REL_EXP_TYP
group by cod_gl_group, cod_acco_cent, cod_expense
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_REL_REG_DIMENSIONS', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', idf_cli, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select idf_cli, count(*) as num from MIS_PAR_REL_REG_DIMENSIONS
where isnull(idf_cli,'') <> ''
group by idf_cli
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_REL_ECO_GRO_REG', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', idf_cli, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select idf_cli, count(*) as num from MIS_PAR_REL_ECO_GRO_REG
group by idf_cli
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_CAT_ECO_GRO', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', id_eco_gro, 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select id_eco_gro, count(*) as num from MIS_PAR_CAT_ECO_GRO
group by id_eco_gro
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_CAT_ECO_GRO_REG', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', id_eco_gro_reg, 'N/A', 'N/A', 'N/A', cast(num as int) from(
select id_eco_gro_reg, count(*) as num from MIS_PAR_CAT_ECO_GRO_REG
group by id_eco_gro_reg
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_CAT_CONV', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cod_conv, 'N/A', 'N/A', cast(num as int) from(
select cod_conv, count(*) as num from MIS_PAR_CAT_CONV
group by cod_conv
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_CAT_MUL_LAT', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', ind_mul_lat, 'N/A', cast(num as int) from(
select ind_mul_lat, count(*) as num from MIS_PAR_CAT_MUL_LAT
group by ind_mul_lat
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_CAT_OFFI', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cod_offi, cast(num as int) from(
select cod_offi, count(*) as num from MIS_PAR_CAT_OFFI
group by cod_offi
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_TTI_SPE', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', idf_cto, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select idf_cto, count(*) as num from mis_par_tti_spe where data_date = '${var:periodo}'
group by idf_cto
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_CAT_PRODUCT', 'N/A', cod_product, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select cod_product, count(*) as num from mis_par_cat_product
group by cod_product
having count(*) > 1
)a;

insert into mis_val_close_dup
select 'MIS_PAR_CAT_SUBPRODUCT', cod_entity, 'N/A', cod_subproduct, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', cast(num as int) from(
select cod_entity, cod_subproduct, count(*) as num from mis_par_cat_subproduct
group by cod_entity, cod_subproduct
having count(*) > 1
)a;