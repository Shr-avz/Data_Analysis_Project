use tables;
select * from indo;

create table indo_stage
LIKE indo;

select * from indo_stage;

insert indo_stage
select *
from indo;

select *,
row_number() over(
partition by iso_code, country, 'year', coal_prod_change_pct) as row_num
from indo_stage;

with duplicate_cte as 
(
select *,
row_number() over(
partition by iso_code, country, 'year', coal_prod_change_pct) as row_num
from indo_stage
)
select * 
from duplicate_cte
where row_num > 1;

select count(*) from indo;

show tables;

set @rowindex := -1;
select @rowindex := @rowindex + 1 as
rowindex, coal_prod_change_pct as indo_coal
from indo
order by coal_prod_change_pct;



select 
avg(N.indo_coal)
from
(select @rowindex := @rowindex + 1 as
rowindex, coal_prod_change_pct as indo_coal
from indo
order by coal_prod_change_pct) as N 
where 
rowindex in (floor(@rowindex/2),
ceil(@rowindex / 2));

SELECT CONCAT(
    'UPDATE indo SET ',
    GROUP_CONCAT(
        CONCAT(column_name, ' = NULLIF(', column_name, ', '')')
    ),
    ';'
)
INTO @sql
FROM information_schema.columns
WHERE table_name = 'indo set';

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


select * from indo;

DELIMITER //

CREATE PROCEDURE replace_empty_with_null()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE col_name VARCHAR(255);
    DECLARE cur CURSOR FOR 
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'indo';

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO col_name;
        IF done THEN
            LEAVE read_loop;
        END IF;
        
        SET @sql = CONCAT('UPDATE indo SET ', col_name, ' = NULLIF(', col_name, ', "")');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;

    CLOSE cur;
END //

DELIMITER ;

select * from indo

