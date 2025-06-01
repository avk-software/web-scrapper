<?php

function handler($event, $context) 
{   
    $dataString = json_decode($event['body'], true);
    $status = $dataString['resource']['status'];
    $actorId = $dataString['eventData']['actorId'];

    if ($status == 'SUCCEEDED' && $actorId == "moJRLRc85AitArpNN")
    {
        $taskName = "currency-rates-scrapper";
    }
    else
    {
        $data = $dataString['resource']['status'];
    }
        $token = getenv('APIFY_TOKEN');
        $curl = curl_init();
        curl_setopt($curl, CURLOPT_URL, "https://api.apify.com/v2/actor-tasks/r_express~currency-rates-scrapper/runs/last/dataset/items?token=apify_api_LDVIIarkvrbVj4qg9q8Vd1yuuz87HC3kPXQs");
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);

        if (!($result = curl_exec($curl)))
        {
            $result = curl_error($curl);
        }
        curl_close($curl);

        $result = json_decode($result, true);
        var_dump($result);

        $responseArray = array();

        foreach ($result as $touroperatorData)
        {
            foreach ($touroperatorData['data'] as $data)
            {
                $currencyId = $data['id'];

                // Исключение для ТО "Гранд Экспресс"
                if ($currencyId == 17613)
                {
                    $rate = $data['rate'][1];
                }
                else
                {
                    $rate = $data['rate'];
                }
                         
                $cb = $data['% к ЦБ'];
                $delta = $data['Δ, руб.'];

                $queryUrl = 'https://megapolus.bitrix24.ru/rest/11/pbjl5ed8q1303lh0/bizproc.workflow.start';
        	
			    $params = array(
				    'TEMPLATE_ID' => 617,
                    'DOCUMENT_ID' => ['lists', 'Bitrix\Lists\BizprocDocumentLists', $currencyId],
                    'PARAMETERS' => array(
                        'rate' => $rate,
                        'cb' => $cb,
                        'delta' => $delta
                    )
			    );

    		    $curl = curl_init($queryUrl);

			    curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);
    		    curl_setopt($curl, CURLOPT_POST, true);
    		    curl_setopt($curl, CURLOPT_POSTFIELDS, http_build_query($params));

    		    $response = curl_exec($curl);
    		    $response = json_decode($response, true);

                $responseArray[] = $response;    
            }
        }          

        $data = json_encode($responseArray);

    return [
        "statusCode" => 200,
        "body" => $data
    ];
}