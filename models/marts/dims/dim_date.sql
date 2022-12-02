select
    *
from 
    unnest(GENERATE_DATE_ARRAY('2020-01-01', '2022-04-07', INTERVAL 1 day)) as `date`