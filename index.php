<?php

    $db = mysqli_connect('ccinstance.c1mcasvph2xx.us-east-1.rds.amazonaws.com','srinath','uppal143','commoncrawl')
    or die('Error connecting to MySQL server.');
    $URL = $_GET['URL'];
    /*$Sno = $_GET['sno'];
    $query = "SELECT * FROM serverName where Sno = $Sno";*/
    $query = "SELECT * FROM serverName where URL like '%$URL%'";
    
    $result = mysqli_query($db, $query);
    
    echo "<table border='1'>
    <tr>
    <th>Sno</th>
    <th>URL</th>
    <th>Server</th>
    <th>Cookies</th>
    <th>XFO</th>
    <th>CSP</th>
    <th>PKP</th>
    <th>STS</th>
    <th>XCT</th>
    <th>X-XSS</th>
    <th>X-Download</th>
    <th>CDP</th>
    <th>Drupal</th>
    <th>Drupal Ver</th>
    <th>Joomla</th>
    <th>Joomla Version</th>
    <th>Wordpress</th>
    <th>Wordpress Version</th>
    <th>JQuery</th>
    <th>JQuery Version</th>
    </tr>";
    
    while($row = mysqli_fetch_array($result))
    {
    echo "<tr>";
    echo "<td>" . $row['Sno'] . "</td>";
    echo "<td>" . $row['URL'] . "</td>";
    echo "<td>" . $row['server'] . "</td>";
    echo "<td>" . $row['setcookie'] . "</td>";
    echo "<td>" . $row['xfo'] . "</td>";
    echo "<td>" . $row['csp'] . "</td>";
    echo "<td>" . $row['pkp'] . "</td>";
    echo "<td>" . $row['sts'] . "</td>";
    echo "<td>" . $row['XCT'] . "</td>";
    echo "<td>" . $row['xxss'] . "</td>";
    echo "<td>" . $row['xdown'] . "</td>";
    echo "<td>" . $row['cdp'] . "</td>";
    echo "<td>" . $row['drupal'] . "</td>";
    echo "<td>" . $row['drupalver'] . "</td>";
    echo "<td>" . $row['joomla'] . "</td>";
    echo "<td>" . $row['joomlaver'] . "</td>";
    echo "<td>" . $row['wordpress'] . "</td>";
    echo "<td>" . $row['wordpressver'] . "</td>";
    echo "<td>" . $row['jquery'] . "</td>";
    echo "<td>" . $row['jqueryver'] . "</td>";
    echo "</tr>";
    }
    echo "</table>";
    mysqli_close($db);
?>