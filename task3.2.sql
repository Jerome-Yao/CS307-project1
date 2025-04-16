select count(*) from sales
group by gender;

select supply_center, count(distinct c.client_name),count(distinct c.country) ,count(distinct s.salesman_number)from supply_center sc
join client c on sc.center_name = c.supply_center
join contract ct on c.client_name = ct.client_name
join order_detail od on ct.contract_number = od.contract_number
join sales s on od.salesman_number = s.salesman_number
group by supply_center;

select cl.country,cl.client_name,cl.industry from client cl
where cl.country='Italy';

select cl.country,cl.client_name,cl.industry from client cl
where cl.country='Canada';


select * from product_model
where product_code='B56K283';

select * from product_model
where product_code='C78629B';


select ct.contract_number,product_code,product_model,quantity,salesman_name,lodgement_date from contract ct
join order_detail od on ct.contract_number = od.contract_number
join sales s on od.salesman_number = s.salesman_number;
