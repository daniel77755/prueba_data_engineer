select count(cliente_id) conteo_clientes from retailtech.clientes where aplica_retencion_dias = 'no'; -- Esta consulta permite obtener el total de clientes que existen en la tabla clientes, el impacto hacia negocio es conocer cuantos clientes existen hasta la fecha.

select count(c.cliente_id) cantidad_clientes_B2B_compras_mayores_500000 from retailtech.clientes c
inner join (select pedido_id, cliente_id, fecha_pedido, total_neto, row_number() over (partition by pedido_id, cliente_id, fecha_pedido) unicos from retailtech.pedidos where estado = 'entregado' and aplica_retencion_dias = 'no') p
on c.cliente_id = p.cliente_id
where c.segmento = 'B2B' and p.total_neto > 500000 and unicos = 1; -- Esta consulta permite conocer la cantidad de clientes B2B que realizaron compras por encima de $500,000 COP, el impacto hacia negocio es conocer un segmento de clientes con una capacidad adquisitividad buena.

select c.canal as canal_con_mejor_tasa_conversion from (select canal, estado, count(*) conteo_con_mejor_tasa_conversion from (select canal, estado, row_number() over (partition by pedido_id, cliente_id, fecha_pedido) unicos from retailtech.pedidos 
where estado = 'entregado' and aplica_retencion_dias = 'no') p where unicos = 1 group by canal, estado) c order by conteo_con_mejor_tasa_conversion desc limit 1; -- Esta consulta permite conocer que canal tiene la mejor tasa de conversion, el impacto hacia negocio es conocer que aspectos de ese canal sirven para los otros canales.

select nomprod.nombre_producto from (select prmv.nombre_producto, sum(prmv.cantidad_productos_vendidos) cantidad_productos_vendidos from (select pr.nombre_producto, date_format(p.fecha_pedido, '%Y-%m-01') as mes, sum(dp.cantidad) cantidad_productos_vendidos
from (select pedido_id, fecha_pedido, estado, row_number() over (partition by pedido_id, cliente_id, fecha_pedido) unicos from retailtech.pedidos where aplica_retencion_dias = 'no') p
left join retailtech.detalle_pedidos dp on p.pedido_id = dp.pedido_id
left join retailtech.productos pr on pr.producto_id = dp.producto_id
where estado = 'entregado' and unicos = 1 group by 1,2
having mes between '2024-06-01' and '2024-12-01') prmv
group by 1 
order by cantidad_productos_vendidos desc 
limit 5) nomprod; -- Esta consulta permite obtener una lista de los nombres de los 5 productos mas vendidos del segundo semestre del 2024, el impacto hacia negocio al reconocer cuales son los productos que el publico les interesa mas.

select pais, count(cliente_id) clientes_por_pais from retailtech.clientes where aplica_retencion_dias = 'no' group by pais; -- Esta consulta permite obtener el total de clientes por país, el impacto hacia negocio es que permite conocer la cantidad de clientes potenciales por pais.

SELECT 
    c.pais,
    COUNT(DISTINCT c.cliente_id)                                    AS clientes_activos,
    COUNT(DISTINCT p.pedido_id)                                     AS total_pedidos,
    ROUND(SUM(p.total_neto), 0)                                     AS ventas_totales_cop,
    ROUND(SUM(p.total_neto) / COUNT(DISTINCT p.pedido_id), 0)       AS ticket_promedio,
    ROUND(SUM(p.total_neto) / COUNT(DISTINCT c.cliente_id), 0)      AS revenue_por_cliente
FROM retailtech.pedidos p
INNER JOIN retailtech.clientes c ON p.cliente_id = c.cliente_id
WHERE c.pais IN ('Colombia', 'México')
  AND p.aplica_retencion_dias = 'no'
  AND p.estado = 'entregado'
GROUP BY c.pais
ORDER BY ventas_totales_cop DESC; -- Esta consulta permite conocer el resumen de ventas para los paises de Colombia y México, el impacto hacia negocio al reconocer como le fueron en ventas a esos dos paises cada año.

select p.estado, count(*) total_pedidos
from (select estado, row_number() over (partition by pedido_id, cliente_id, fecha_pedido) unicos from retailtech.pedidos where aplica_retencion_dias = 'no') p
where p.estado in ('pendiente', 'enviado') and unicos = 1
group by estado
order by total_pedidos desc; -- Esta consulta permite conocer la carga de trabajo actual en la cadena de despacho, el impacto hacia negocio porque permite conocer donde enfocar esfuerzcos

select dv.nombre_producto from (select pr.nombre_producto, date_format(p.fecha_pedido, '%Y-01-01') as anio, sum(dp.cantidad) cantidad_productos_devueltos
from (select pedido_id, fecha_pedido, estado, row_number() over (partition by pedido_id, cliente_id, fecha_pedido) unicos from retailtech.pedidos where aplica_retencion_dias = 'no') p
left join retailtech.detalle_pedidos dp on p.pedido_id = dp.pedido_id
left join retailtech.productos pr on pr.producto_id = dp.producto_id
where estado = 'devuelto' and unicos = 1 group by 1,2
having anio = '2023-01-01' order by cantidad_productos_devueltos desc limit 1) dv; -- Esta consulta permite obtener el nombre del producto mas devuelto durante el año 2023, el impacto hacia negocio al reconocer cual es el producto que mas problemas ha generado a los clientes.

select pais_envio,
round(avg(datediff(fecha_entrega, fecha_pedido)), 1) dias_promedio_entrega
from (select pais_envio, fecha_pedido, fecha_entrega, estado, row_number() over (partition by pedido_id, cliente_id, fecha_pedido) unicos from retailtech.pedidos where aplica_retencion_dias = 'no') p
where p.estado = 'entregado' and p.fecha_entrega is not null and unicos = 1
group by pais_envio
order by dias_promedio_entrega; -- Esta consulta permite conocer el tiempo promedio de entrega por país, el impacto hacia negocio al reconocer cuales son los paises donde se puede mejorar la logística de entrega.

select activo, count(producto_id) estado_productos from retailtech.productos group by activo and aplica_retencion_dias = 'no'; -- Esta consulta permite conocer la cantidad de productos activos e inactivos, el impacto hacia negocio al reconocer cuantos productos estan disponibles para la venta.