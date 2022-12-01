select
    *
from 
    unnest(GENERATE_DATE_ARRAY('2022-04-3', '2022-04-07', INTERVAL 1 day)) as `date`