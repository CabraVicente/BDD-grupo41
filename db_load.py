import db_reset

### El orden es de suma importancia!!! ###

import loaders.cliente
print("Loaded clientes")
import loaders.comuna_direccion # Necesita que exista la tabla Cliente
print("Loaded comunas y direccion")
import loaders.empresadelivery
print("Loaded empresas de delivery")
import loaders.despachador # Necesita que exista la tabla EmpresaDelivery
print("Loaded despachadores y EmpresaDelivery_Despachadores")
import loaders.restaurante_sucursal
print("Loaded restaurantes y sucursales")
import loaders.plato # Necesita que exista la tabla Restaurante
print("Loaded platos")
import loaders.pedido_calificacion # Necesita que exista la tabla Restaurante, Plato y Cliente
print("Loaded Pedido y Calificacion")
import loaders.suscripcion # Necesita que exista la tabla Cliente y EmpresaDelivery
print("loaded sus")
import loaders.despacho # Necesita que exista la tabla Pedido_Plato, Plato, Despachador, Restaurante y Sucursal
print("loaded despachos")