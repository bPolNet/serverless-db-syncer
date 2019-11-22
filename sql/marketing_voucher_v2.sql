DROP TABLE IF EXISTS Voucher_Cost;
DROP TABLE IF EXISTS omegon;
DROP TABLE IF EXISTS omegon2;

CREATE TABLE Voucher_Cost(
    id_order int,
    id_shop int,
    shop_name varchar(255),
    date_add datetime,
    product_id int,
    product_name VARCHAR(200) character set utf8,
    product_quantity int,
    product_price float,
    total_price_tax_incl float,
    total_paid float,
    Business_category_level_1 varchar(255),
    Business_category_level_2 varchar(255),
    Business_category_level_3 varchar(255),
    ABInBev_Company varchar(255),
    Total_Value float,
    Total_Voucher_Order float,
    Product_Quantity_order int,
    Product_Voucher_Cost float,
    Total_Product_Voucher_Cost float,
    Portion_Voucher_per_order float,
    Persona varchar(255),
    year_extract int,
    month_extract int
);

CREATE TABLE omegon(
  id_order int,
  product_qantity int,
  PRIMARY KEY (id_order)
);

CREATE TABLE omegon2(
  id_order int,
  value_sum float,
  PRIMARY KEY (id_order)
);

INSERT INTO Voucher_Cost(
    id_order,
    id_shop,
    shop_name,
    date_add,
    product_id,
    product_name,
    product_quantity,
    product_price,
    total_price_tax_incl,
    total_paid,
    Business_category_level_1,
    Business_category_level_2,
    Business_category_level_3,
    ABInBev_Company,
    Total_Voucher_Order
)
        SELECT psod.id_order,
               pso.id_shop,
               ps.name,
               pso.date_add,
               psod.product_id,
               plang.name ,
               psod.product_quantity,
               psod.product_price,
               psod.total_price_tax_incl,
               pso.total_paid,
               levelone.code ,
               leveltwo.code ,
               levelthree.code ,
               abi_table.abi ,
               table_voucher.total_voucher_per_order
        FROM ps_order_detail psod
        INNER JOIN ps_orders pso on (pso.id_order=psod.id_order)
        INNER JOIN ps_shop ps on (pso.id_shop = ps.id_shop)
        LEFT JOIN (
             Select pbc.id_product,bc.code
              from business_category bc
              inner join product_business_category pbc on pbc.id_business_category=bc.id
              where level= 1
          ) levelone on levelone.id_product= psod.product_id
        LEFT JOIN (
             Select pbc.id_product,bc.code
              from business_category bc
              inner join product_business_category pbc on pbc.id_business_category=bc.id
              where level= 2
          ) leveltwo on leveltwo.id_product= psod.product_id
        LEFT JOIN (
             Select pbc.id_product,bc.code
              from business_category bc
              inner join product_business_category pbc on pbc.id_business_category=bc.id
              where level= 3
          ) levelthree on levelthree.id_product= psod.product_id
        LEFT JOIN (
            Select distinct pspl.id_product,pspl.name
            from ps_product_lang pspl
            where pspl.id_lang = 4
              And pspl.id_shop = 1
          ) plang on plang.id_product = psod.product_id
        LEFT JOIN (
          Select psp.id_product,
               psp.id_manufacturer,
               psm.abi_company,
               if(psm.abi_company = 1,'Vrai','Faux') as abi
        from ps_product psp
        left join ps_manufacturer psm on psp.id_manufacturer = psm.id_manufacturer
          ) abi_table on abi_table.id_product = psod.product_id
        LEFT JOIN (
                  SELECT
                    ocr.id_order,
                    sum(ocr.value) as total_voucher_per_order
                  FROM ps_order_cart_rule ocr
                  INNER JOIN ps_cart_rule cr on (cr.id_cart_rule=ocr.id_cart_rule)
                  INNER join ps_orders pso on (pso.id_order=ocr.id_order)
                  WHERE cr.code not like '%DRIVE%'
                      And cr.code not like '%CUS%'
                      And cr.code not like 'RF%'
                      And cr.code not like 'CC%'
                      And cr.code not in ('HOPHOPHOP','BREWHOP','HOPVIP','LIVRAISONFUT','LIVRAISON', 'TRIPEL', 'TRIPELFUT','3BIERES', 'LEFFEPAPA', 'TOPDAD', 'NEWBEER', 'GIFT10', 'PERFECTDRAFT19', 'FRANCE', 'HOPTDEAL')
                  And year(pso.date_add) = 2019
                  And month(pso.date_add) = 6
                  GROUP BY  ocr.id_order
                  ORDER BY ocr.id_order asc
          ) table_voucher on table_voucher.id_order=psod.id_order
        WHERE year(pso.date_add) = 2019
          And month(pso.date_add) = 6
          And pso.order_type_id not in (2,7)
          And leveltwo.code not in ('service')
        ORDER BY psod.id_order ASC, psod.product_id ASC;


/** Clause d'update **/

UPDATE Voucher_Cost
    SET year_extract = year(date_add);

UPDATE Voucher_Cost
    SET month_extract = month(date_add);

INSERT INTO omegon(id_order,product_qantity)
  Select Voucher_Cost.id_order,sum(Voucher_Cost.product_quantity)
  From Voucher_Cost
  Group by Voucher_Cost.id_order;

UPDATE Voucher_Cost
  INNER JOIN omegon on omegon.id_order= Voucher_Cost.id_order
    SET Voucher_Cost.Product_Quantity_order = omegon.product_qantity;

INSERT INTO omegon2(id_order,value_sum)
  Select Voucher_Cost.id_order,sum(Voucher_Cost.total_price_tax_incl)
  From Voucher_Cost
  Group by Voucher_Cost.id_order;

UPDATE Voucher_Cost
  INNER JOIN omegon2 on omegon2.id_order= Voucher_Cost.id_order
    SET Voucher_Cost.Total_Value = omegon2.value_sum;

UPDATE Voucher_Cost
  SET Voucher_Cost.Product_Voucher_Cost =
            CASE
              WHEN Voucher_Cost.Total_Voucher_Order is NUll THEN 0
              WHEN Voucher_Cost.Total_Voucher_Order is not NUll THEN Voucher_Cost.Total_Voucher_Order / Voucher_Cost.Product_Quantity_order
            END
;

UPDATE Voucher_Cost
  SET Voucher_Cost.Total_Product_Voucher_Cost = Product_Quantity_order * Product_Voucher_Cost;

UPDATE Voucher_Cost
  SET Portion_Voucher_per_order = Total_Voucher_Order * ( total_price_tax_incl / Total_Value);

UPDATE Voucher_Cost
  SET Persona = CASE
    /** Gestion des Bottles = Beerrs **/
      WHEN Business_category_level_1='beers' and ABInBev_Company='Vrai' THEN 'BOTTLE BUYERS - ABINBEV'
      WHEN Business_category_level_1='beers' and ABInBev_Company='Faux' THEN 'BOTTLES BUYERS - NON ABINBEV'
    /** Box découvertes **/
      WHEN Business_category_level_1='subscription' THEN 'BOX BUYERS'
    /** Gifts **/
      WHEN Business_category_level_1='gifts' THEN 'GIFTERS'
    /** Homebrowing **/
      WHEN Business_category_level_2='kits' THEN 'HBB'
      WHEN (Business_category_level_2='ingredients' or Business_category_level_2='equipment') THEN 'MB'
    /** Kegs **/
      WHEN Business_category_level_1='kegs' THEN 'KEG BUYERS'
    /** Machines, glasses and other**/
      WHEN Business_category_level_1='machines' THEN 'MACHINE BUYERS'
      WHEN Business_category_level_1='glasses' THEN 'GLASSES BUYERS'
      WHEN Business_category_level_1='beery christmas' THEN 'BEERY CHRISTMAS LOVERS'
      ELSE 'TBD'
    END
;