-- считаем разницу времени в часах между предыдущим и следующим для каждого действия 
with vimbox_pages_hdiffs as (
	select
		a.*,
		extract(epoch from (
			lead(a.action_date, 1) over (partition by a.id order by a.action_date) - a.action_date)) / 3600.00 as hours_diff_next_curr,
		extract(epoch from (
			a.action_date - lag(a.action_date, 1) over (partition by a.id order by a.action_date))) / 3600.00 as hours_diff_curr_prev
	from vimbox_pages a
),
-- проставляем флаг начала и флаг конца сессии
vimbox_pages_flags as (
	select
		a.*,
		case when a.hours_diff_curr_prev >= 1 or a.hours_diff_curr_prev is null then 1 else 0 end as start_flag,
		case when a.hours_diff_next_curr >= 1 or a.hours_diff_next_curr is null then 1 else 0 end as end_flag
	from vimbox_pages_hdiffs a
),
-- сквозная нумерация через скользящую сумму начал и концов сессий (если у сессии номер начала 1, то и номер конца 1 и т.д)
vimbox_pages_start_end_rn as (
	select
		a.*,
		case when a.start_flag = 1
             then sum(start_flag) over (partition by a.id order by a.action_date) 
		end as start_rn,
		case when a.end_flag = 1
             then sum(end_flag) over (partition by a.id order by a.action_date) 
		end as end_rn
	from vimbox_pages_flags a
),
-- джоиним предыдущую таблицу саму с собой по id и номер начала = номер конца чтобы получить все сессии, нумеруем их
all_sessions as (
	select
		a.id,
		a.action_date as start_date,
		b.action_date as end_date,
		row_number() over (order by a.action_date) as session_num
	from vimbox_pages_start_end_rn a 
	join vimbox_pages_start_end_rn b
		on a.id = b.id
		and a.start_rn = b.end_rn
		and a.start_rn is not null
),
-- исходную таблицу джоиним с предыдущей чтобы получить все посещенные страницы за сессию
all_sessions_w_pages as (
	select
		b.id,
		b.session_num,
		b.start_date, 
		b.end_date + interval '1 hour' as end_date,
		string_agg(a.page, ', ' order by a.action_date) as visited_pages
	from vimbox_pages a
	join all_sessions b
		on a.id = b.id
		and a.action_date between b.start_date and b.end_date
	group by b.id, b.session_num, b.start_date, b.end_date
)
-- отбираем нужные сессии
select 
	id,
	start_date,
	end_date
from all_sessions_w_pages
where visited_pages like '%rooms.homework-showcase%rooms.view.step.content%rooms.lesson.rev.step.content%'
order by session_num;