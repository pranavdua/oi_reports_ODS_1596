select * from `__project__.__dataset__.__table__`
where CAST(EXTRACT(DATE FROM report_publish_date ) as string) = CAST(current_date('UTC') as string)
