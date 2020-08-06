/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package bbdd;

/**
 *
 * @author Rocio Montalvo
 *
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
 import java.sql.*;

import java.util.logging.Level;

import java.util.logging.Logger;

public class Bddd {
    /**

     * @param args the command line arguments

     */

    public static void main(String[] args) throws IOException {

        try {

            System.out.println("probando si el driver postgreSQL está presente.");
            // FASE 1 : Cargar el drive
            try {
              Class.forName("org.postgresql.Driver");

            } catch (ClassNotFoundException cnfe) {

              System.out.println("driver no disponible!");

              System.out.println("error y salida.");

              cnfe.printStackTrace();

              System.exit(1);

            }

            System.out.println("Driver cargado, empieza la conexión.");
            // FASE 2: Conectar con el servidor
            Connection c = null;
try {
              // se deben ajustar los parámetros para la conexión según la configuración de cada uno
     System.out.print("Escriba el usuario:");         
    InputStreamReader isr = new InputStreamReader(System.in);
BufferedReader br = new BufferedReader (isr);
String Usuario = br.readLine();

   System.out.print("Escriba la contraseña:"); 
InputStreamReader pass = new InputStreamReader(System.in);
BufferedReader br2 = new BufferedReader (pass);
String Clave = br2.readLine();
              c = DriverManager.getConnection("jdbc:postgresql://localhost/Vodafone",Usuario, Clave);
            } catch (SQLException se) {
              System.out.println("No se pudo realizar la conexión.");
              se.printStackTrace();
              System.exit(1);
            }
            if (c != null)
              System.out.println("Conectados a la base de datos!");
            else
              System.out.println("Ha habido un problema no esperado.");
              // FASE 3: Ejecutamos una consulta

              Statement s = null;

              try {

                s = c.createStatement();

              } catch (SQLException se) {

                System.out.println("problema al crear la consulta:" +"seguimos conectados??.");

                se.printStackTrace();

                System.exit(1);

              }

              ResultSet rs = null;

              try {
int op = 0;
    System.out.println("1. Total  facturado por la empresa Vodafone desde la implementación de la base de datos.\n" +
"2. Importe total de voz, el importe total de mensajes enviados y el importe total de datos e internet, consumido por cada uno de los clientes en el último mes para cada línea contratada.\n" +
"3. Total del dinero facturado a cada uno de los clientes desde que el cliente ha contratado algún servicio en Vodafone. \n" +
"4. Plan de precios que más ha sido contratado por todos los clientes dados de alta.\n" +
"5. Total facturado por la empresa debido a cada uno de los tipos de consumo: voz, mensajes e internet.\n" +
"6. Importe total consumido de cada una de las líneas \n" +
"7. Número de teléfono que ha recibido más llamadas desde cualquierterminal de la empresa Vodafone.\n" +
"8. Número de llamadas, mensajes y conexiones a internet que ha realizado cada uno de los clientes \n" +
"9. Nombre de los clientes de la provincia de Madrid que tienen más de una línea contratada y hayan renovado algún teléfono móvil \n" +
"10. Media diaria de importe de factura de cada uno de los clientes al mes.\n" +
"11. Nombre de los clientes que han gastado al mes en alguna de sus facturas más de 50 euros.\n" +
"12. Clientes que no han realizado ninguna conexión de datos o internet en alguna de sus líneas contratadas.\n" +
"13. Clientes que no han enviado ningún mensaje desde su línea en su última factura recibida.\n" +
"14. Clientes que han enviado más de 10 mensajes en su última factura.\n" +
"15. Forma de pago más utilizada por los clientes de Vodafone.\n" +
"16. Número de minutos que ha utilizado cada cliente su línea telefónica para realizar llamadas de voz en el último mes.\n" +
"17. Marca y modelo del teléfono móvil que más se ha canjeado en el último mes.\n" +
"18. Importe de IVA que la empresa Vodafone ha recaudado en total en cada uno de los meses que la empresa ha prestado sus servicios.\n" +
"19. Facturación total por comunidades que ha realizado la empresa.\n" +
"20. Provincia que realiza más llamadas de voz hasta el momento.");
 switch ( op ) {
case 1:
 rs = s.executeQuery("SELECT SUM ( Total )FROM  Factura "); 
 break;
case 2:
rs = s.executeQuery("select  Numero , Sum( Importe ),max(date_part( 'MONTH' , Fecha_inicio ) )from  Voz  GROUP BY  Numero select  Numero , Sum( Importe ),max(date_part( 'MONTH' , Fecha_inicio ) )from  Mensaje  GROUP BY  Numero  "); 
 break; 
case 3:
rs = s.executeQuery("SELECT  DNI , Nombre , Apellidos FROM  Cliente ,  Factura WHERE  Cliente . Codigo_Cliente = Factura . Codigo_Cliente GROUP by  Cliente . Codigo_Cliente  ORDER BY SUM( Total ) desc"); 
 break;
case 4:
rs = s.executeQuery("SELECT max( Nombre )  FROM  Linea "); 
 break;
case 5:
rs = s.executeQuery("SELECT SUM( Voz . Importe ),SUM( Mensaje . Importe ),SUM( Datos . Importe ) FROM  Voz ,  Mensaje ,  Datos "); 
 break;
 case 6:
rs = s.executeQuery("select  Codigo_Cliente  , Consumo . Numero ,Sum( Importe )from  Linea , Consumo  GROUP BY  Consumo . Numero , Codigo_Cliente "); 
 break;
 case 7:
rs = s.executeQuery("SELECT MAX( Numero_destino )FROM  Voz "); 
 break;
 case 8:
rs = s.executeQuery("select  DNI  , Linea . Numero ,count( Voz . Numero ),count( Mensaje . Numero ),count( Datos . Numero )from  Cliente , Linea ,  Voz , Mensaje , Datos GROUP BY  Linea . Numero , DNI "); 
 break;
 case 9:
rs = s.executeQuery("select  DNI  , Nombre , Apellidos , Cliente_puntos . Modelo , Marca from  Cliente , Plan_de_puntos , Cliente_puntos WHERE  Cliente . Provincia ='madrid'GROUP BY DNI , Nombre , Apellidos , Cliente_puntos . Modelo , Marca "); 
break;
 case 11:
rs = s.executeQuery("SELECT  Nombre , Total  FROM  Cliente  , Factura  WHERE  Total >'50'"); 
rs = s.executeQuery("SELECT max( Tipo )FROM  Forma_de_pago  "); 
 break;
case 16:
rs = s.executeQuery("SELECT max( Tipo )FROM  Forma_de_pago "); 
 break;
 case 17:
rs = s.executeQuery(" SELECT Cliente_puntos . Modelo ,Count( Cliente_puntos . Modelo ), max(date_part( 'MONTH' , Fecha_inicio ) )FROM  Cliente_puntos group by  Cliente_puntos . Modelo  ) )"); 
 break;
 case 18:
rs = s.executeQuery("SELECT SUM( IVA ),(date_part( 'MONTH' , Fecha_de_emision ) )FROM  Factura GROUP BY  Fecha_de_emision ");        
 case 19:
rs = s.executeQuery("SELECT  Comunidad_autonoma ,SUM( IVA ) FROM  Cliente , Factura GROUP BY  Comunidad_autonoma "); 
 break;
 case 20:
rs = s.executeQuery("SELECT Comunidad_autonoma,Count(Voz.Numero) FROM Cliente,Voz GROUP BY Comunidad_autonoma");
break;
 default:
     System.out.println("error");
 break;
 }
 

               

              } catch (SQLException se) {

                System.out.println("Excepción al ejecutar consulta:" +"probablemente tenemos un error de sintaxis en el SQL");

                se.printStackTrace();

                System.exit(1);

              }
              //FASE 4: Mostrar la información en pantalla
              int index = 0;
              try {
                while (rs.next()) {
                    System.out.println("Resultado de la fila " + index++ + ":");
                    System.out.println(rs.getString("nombre"));
                    System.out.println(String.valueOf(rs.getInt("id")));
                }
              } catch (SQLException se) {
                System.out.println("Error grave al recoger los resultados... " );
                se.printStackTrace();
                System.exit(1);
              }
               rs.close();
              s.close();
              c.close();
        } catch (SQLException ex) {

            Logger.getLogger(Bddd.class.getName()).log(Level.SEVERE, null, ex);

        }
    }
}
