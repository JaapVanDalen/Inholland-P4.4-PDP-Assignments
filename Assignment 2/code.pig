-- Get CSVExcelStorage
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage; 

-- Load the CSV file from orders.csv
order_list = LOAD '/user/maria_dev/diplomacy/orders.csv' USING CSVExcelStorage() AS
(game_id:int,
unit_id:int,
unit_order:chararray,
location:chararray,
target:chararray,
target_dest:chararray,
success:int,
reason:int,
turn_num:int);

-- Filter where target is 'Holland'. 
-- We do this before to make the Group query faster
order_list_filtered = FILTER order_list BY target == 'Holland';

-- Group by "location" 
order_list_grouped = GROUP order_list_filtered BY (location, target);

--Count how many times Holland was the target from that location
order_list_unordered = FOREACH order_list_grouped GENERATE group, COUNT(order_list_filtered);

-- Make a alphabetic list from all locations from the orders.csv
order_list_ordered = ORDER order_list_unordered BY $0 ASC;

-- Print with DUMP-statement:
DUMP order_list_ordered;