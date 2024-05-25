import db_reset

### El orden es de suma importancia!!! ###

import loaders.cliente
print("Loaded clientes")
import loaders.comuna_direccion # Necesita que exista la tabla Cliente
print("Loaded comunas y direccion")
import loaders.empresadelivery
print("Loaded empresas de delivery")
import loaders.despachador
print("Loaded despachadores")
import loaders.restaurante_sucursal
print("Loaded restaurantes y sucursales")
import loaders.plato # Necesita que exista la tabla Restaurante
print("Loaded platos")
import loaders.pedido_calificacion # Necesita que exista la tabla Restaurante, Plato y Cliente
print("Loaded Pedido y Calificacion")
import loaders.suscripcion_3 # Necesita que exista la tabla Cliente y EmpresaDelivery
print("loaded sus")