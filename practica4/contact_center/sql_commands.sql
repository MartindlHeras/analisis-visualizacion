delete from contact_center where sessionID is null;
select sessionid, dni, telef, cp, duration_call_mins, producto,
    (case
        when group_concat(funnel_Q) like '%chalet%' then 'chalet'
        when group_concat(funnel_Q) like '%piso%' then 'piso'
        else ''
    end) as chalet_piso,
    (case
        when group_concat(funnel_Q) like '%bajo%' then 'bajo'
        when group_concat(funnel_Q) like '%intermedio%' then 'intermedio'
        else ''
    end) as bajo_intermedio,
    (case
        when group_concat(funnel_Q) like '%adosado%' then 'adosado'
        when group_concat(funnel_Q) like '%unifamiliar%' then 'unifamiliar'
        else ''
    end) as adosado_unifamiliar,
    (case
        when group_concat(funnel_Q) like '%con rejas%' then 'con rejas'
        when group_concat(funnel_Q) like '%sin rejas%' then 'sin rejas'
        else ''
    end) as rejas,
    (case
        when group_concat(funnel_Q) like '%con perro%' then 'con perro'
        when group_concat(funnel_Q) like '%sin perro%' then 'sin perro'
        else ''
    end) as perro
from contact_center group by sessionID, dni, telef, cp, duration_call_mins, producto;

delete from renta_por_hogar where periodo < 2019;
delete from renta_por_hogar where `indicadores de renta media y mediana`!='renta neta media por hogar';
delete from renta_por_hogar where secciones is not null;
delete from renta_por_hogar where distritos is null;
alter table renta_por_hogar drop column secciones;

-- delitos_por_municipio editamos los nombres de las columnas antes de meter la tabla
delete from delitos_por_municipio where municipio like '%comunidad de%';
alter table delitos_por_municipio drop column `Unnamed: 31`;
update delitos_por_municipio set municipio = replace(municipio, '- Municipio de ', '');

select c.*, r.*, d.* from contact_center as c 
join renta_por_hogar as r on r.municipios like concat('%', c.cp, '%') 
join delitos_por_municipio as d on r.municipios like concat('%', d.municipio, '%');
