CREATE TABLE `data-lab-api.detectionProject_005.extract_005_ligtic` AS 

WITH
TICKET AS (
   SELECT mnt_ht,
        num_art,
        num_rgrpcli,
        dat_vte,
        NUM_ETT,
        NUM_TYPETT,
        QTE_ART,
        PRX_VTEMAG,
        PRX_ACH,
        NUM_TYPTRN,
        COD_EANART,
        mnt_ttcdevbu
        --cod_cartfid,
   FROM  `ddp-data-hacking-days.BV_PROD_005_PBSDBS.TF005_VTE_LIGTICCAI`
     WHERE  dat_vte > date_add(CURRENT_DATE(), INTERVAL -3 YEAR)
        AND num_rgrpcli is not NULL
   ),
RAYON AS (
    SELECT num_art, num_ray, num_sray, num_typ, num_styp
   FROM `ddp-data-hacking-days.BV_PROD_005_PBSDBS.TA005_RAR_ART`
),
NOMENCLATURE AS (
SELECT   A.NUM_ART, DAT_PRMIMPL,
      LA.LIB_ART, A.NUM_RAY ,LIB_RAY , A.NUM_SRAY ,LIB_SRAY , A.NUM_TYP ,   LIB_TYP ,  A.NUM_STYP , LIB_STYP,
      CONCAT(cast(cast(A.NUM_RAY as INT64) as STRING) , ' - ' , LIB_RAY) as NUM_LIB_DEPT,
      CONCAT(cast(cast(A.NUM_SRAY as INT64) as STRING) , ' - ' , LIB_SRAY) as NUM_LIB_SDEPT,
      CONCAT(cast(cast(A.NUM_TYP as INT64) as STRING) , ' - ' , LIB_TYP) as NUM_LIB_TYP,
      CONCAT(cast(cast(A.NUM_STYP as INT64) as STRING) , ' - ' , LIB_STYP) as NUM_LIB_STYP,
      CONCAT(cast(cast(A.NUM_ART  as INT64) as STRING) , ' - ' , LIB_ART) as NUM_LIB_ART

  FROM `ddp-data-hacking-days.BV_PROD_005_PBSDBS.TA005_RAR_ART` A
  LEFT  JOIN  `ddp-data-hacking-days.BV_PROD_005_PBSDBS.TD005_RAR_LIBART` LA
    ON
      A.num_cen = LA.num_cen
      AND A.num_art = LA.num_art
  LEFT  JOIN
  (
      SELECT   LR.COD_LAN ,LR.NUM_RAY ,LR.LIB_RAY ,LSR.NUM_SRAY ,LSR.LIB_SRAY , LTY.NUM_TYP ,   LTY.LIB_TYP ,  SLTY.NUM_STYP , SLTY.LIB_STYP
      FROM `ddp-data-hacking-days.BV_PROD_005_PBSDBS.TD005_NCA_LIBSTYP`   SLTY
       INNER JOIN  `ddp-data-hacking-days.BV_PROD_005_PBSDBS.TD005_NCA_LIBTYP`   LTY
      ON
          SLTY.num_cen = LTY.num_cen
          AND SLTY.NUM_RAY  = LTY.NUM_RAY
          AND SLTY.NUM_TYP  = LTY.NUM_TYP
          AND SLTY.NUM_SRAY  = LTY.NUM_SRAY
          AND LTY.COD_LAN =SLTY.COD_LAN
       INNER JOIN  `ddp-data-hacking-days.BV_PROD_005_PBSDBS.TD005_NCA_LIBSRAY`   LSR
      ON
          LTY.num_cen = LSR.num_cen
          AND LTY.NUM_RAY  = LSR.NUM_RAY
          AND LTY.NUM_SRAY  = LSR.NUM_SRAY
          AND LSR.COD_LAN =LTY.COD_LAN
      INNER JOIN  `ddp-data-hacking-days.BV_PROD_005_PBSDBS.TD005_NCA_LIBRAY`   LR
      ON
          LSR.num_cen = LR.num_cen
          AND LSR.NUM_RAY  = LR.NUM_RAY
          AND LSR.COD_LAN = LR.COD_LAN
      WHERE
          SLTY.COD_STA = 1
          AND LTY.COD_STA = 1
          AND LSR.COD_STA = 1
          AND LR.COD_STA = 1
  ) AS  NOMENC
  ON
      a.NUM_RAY = NOMENC.NUM_RAY
      AND a.NUM_SRAY = NOMENC.NUM_SRAY
      AND a.NUM_TYP = NOMENC.NUM_TYP
      AND a.NUM_STYP = NOMENC.NUM_STYP
      AND LA.COD_LAN = NOMENC.COD_LAN
  WHERE LA.COD_LAN = 'IT')
SELECT 
    RAYON.num_ray as num_ray,
    RAYON.num_sray as num_sray,
    RAYON.num_typ as num_typ,
    RAYON.num_styp as num_stype,
    LIB_RAY , LIB_SRAY , LIB_TYP ,
    NOMENCLATURE.LIB_STYP as lib_styp,
    NOMENCLATURE.LIB_ART as lib_art,
    NOMENCLATURE.NUM_LIB_DEPT as num_lib_dept,
    NOMENCLATURE.NUM_LIB_SDEPT as num_lib_sdept,
    NOMENCLATURE.NUM_LIB_TYP as num_lib_typ,
    NOMENCLATURE.NUM_LIB_STYP as num_lib_styp,
    NOMENCLATURE.NUM_LIB_ART as num_lib_art,
    TICKET.mnt_ht as mnt_ht,
    TICKET.num_art as num_art,
    TICKET.dat_vte as date_vte,
    TICKET.num_rgrpcli as num_rgrpcli,
    TICKET.NUM_ETT as num_ett,
    TICKET.NUM_TYPETT as num_typett,
    TICKET.QTE_ART as qte_art,
    TICKET.PRX_VTEMAG as prix_vtemag,
    TICKET.PRX_ACH as prix_achat,
    TICKET.NUM_TYPTRN as num_typtrn,
    TICKET.COD_EANART as cod_eanart,
    TICKET.mnt_ttcdevbu as mnt_ttcdevbu
FROM TICKET
LEFT JOIN RAYON
    ON TICKET.num_art = RAYON.num_art
LEFT JOIN NOMENCLATURE
    ON TICKET.num_art = NOMENCLATURE.num_art

WHERE NOMENCLATURE.NUM_RAY = 7
