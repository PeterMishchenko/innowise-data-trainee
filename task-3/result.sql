
select name, films_cnt 
from category join 
(
    select category_id, count(film_id) as films_cnt 
    from film_category 
    group by category_id 
) as t 
on category.category_id = t.category_id 
order by films_cnt desc;


select first_name, last_name 
from actor join 
(
    select actor_id, sum(rent_cnt) as rent_cnt 
    from film_actor join 
    (
        select film_id, count(rental_id) as rent_cnt 
        from rental join inventory 
        on rental.inventory_id = inventory.inventory_id
        group by film_id
    ) as t 
    on film_actor.film_id = t.film_id
    group by actor_id 
    order by rent_cnt desc
    limit 10) as d 
on actor.actor_id = d.actor_id;


select name 
from category join
(
    select category_id, sum(replacement_cost) as costs 
    from film join film_category on film.film_id = film_category.film_id
    group by category_id 
    order by costs 
    desc limit 1
) as t 
on category.category_id = t.category_id;


select title 
from film left join inventory 
on film.film_id = inventory.film_id
where inventory_id is null;



select first_name, last_name , films_cnt
from actor join 
(
    select actor_id, count(film_id) as films_cnt 
    from film_actor where film_id in (select film_id from film_category where category_id = (select category_id from category where name = 'Children'))
    group by actor_id 
    order by films_cnt desc
) as t
on actor.actor_id = t.actor_id
where films_cnt in (
    select DISTINCT  count(film_id) as films_cnt 
    from film_actor where film_id in (select film_id from film_category where category_id = (select category_id from category where name = 'Children'))
    group by actor_id 
    order by films_cnt desc 
    limit 3)
 ;


select city, active, cnt - active as non_active
from city join
(
    select city_id, count(active) as cnt, sum(active) as active
    from customer join address on customer.address_id = address.address_id
    group by city_id
) as d
on city.city_id = d.city_id
order by non_active desc;
 


select distinct on (gr) gr, name, rental_duration
from (
select gr, name, sum(rental_duration) as rental_duration
from 
(
    select gr, name,
    (extract(epoch from  return_date - rental_date) ) / 3600 as rental_duration
            
    from (
        select city_id, 'group_1' as gr
        from city
        where city like 'a%' or city like 'A%'
        UNION
        select city_id, 'group_2' as gr
        from city
        where city like '%-%' 
    ) as c
    join address on address.city_id = c.city_id
    join customer on address.address_id = customer.address_id
    join rental on rental.customer_id = customer.customer_id
    join inventory on rental.inventory_id = inventory.inventory_id
    join film_category on film_category.film_id = inventory.film_id
    join category on category.category_id = film_category.category_id
) as f
group by gr, name
order by gr, rental_duration desc) as t

