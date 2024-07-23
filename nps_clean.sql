DECLARE data_ref DATE;
SET data_ref=CURRENT_DATE('America/Sao_Paulo');

CREATE OR REPLACE TABLE dataset_temp1.table_elegiveis_iniciais AS (

WITH addition_bonus AS (
		SELECT			
			DISTINCT A.chavecliente,
			1 AS addition,
                        1 AS bonus
		FROM			
			`project1.dataset_views.table_extrato`	A
			LEFT JOIN `project2.dataset_prod2.table_parceiros` B
  		USING (chaveparceiro,chavefilialparceiro)
		WHERE 0=0
			AND DATE(A.DataProcessamentoTransacao,'America/Sao_Paulo') = DATE_ADD(data_ref, INTERVAL -1 DAY) -- processadas em D-1
			AND DATE(A.DataCheckOut,'America/Sao_Paulo') >= DATE_ADD(data_ref, INTERVAL -3 DAY) -- transações feitas recentemente
			AND B.nivel2='varejo'
			AND B.nivel3='bonus'
)
, addition_duplamarca AS (
		SELECT			
			DISTINCT A.chavecliente,
			1 AS addition,
                        1 AS duplamarca
		FROM			
			`project1.dataset_views.table_extrato`	A
			LEFT JOIN `project2.dataset_prod2.table_parceiros` B
  		USING (chaveparceiro,chavefilialparceiro)
		WHERE 0=0
			AND DATE(A.DataProcessamentoTransacao,'America/Sao_Paulo') = DATE_ADD(data_ref, INTERVAL -3 DAY) -- processadas em D-3
			AND DATE(A.DataCheckOut,'America/Sao_Paulo') >= DATE_ADD(data_ref, INTERVAL -5 DAY) -- transações feitas recentemente
			AND B.nivel2='duplamarca'
)
, addition_online AS (
		SELECT			
			DISTINCT A.chavecliente,
			1 AS addition,
                        1 AS online
		FROM			
			`project1.dataset_views.table_extrato`	A
			LEFT JOIN `project2.dataset_prod2.table_parceiros` B
  		USING (chaveparceiro,chavefilialparceiro)
		WHERE 0=0
			AND DATE(A.DataProcessamentoTransacao,'America/Sao_Paulo') = DATE_ADD(data_ref, INTERVAL -1 DAY) -- processadas em D-1
			AND B.nivel2='online'
)
, addition_varejo AS (
		SELECT			
			DISTINCT A.chavecliente,
			1 AS addition,
                        1 AS varejo
		FROM			
			`project1.dataset_views.table_extrato`	A
			LEFT JOIN `project2.dataset_prod2.table_parceiros` B
  		USING (chaveparceiro,chavefilialparceiro)
		WHERE 0=0
			AND DATE(A.DataProcessamentoTransacao,'America/Sao_Paulo') = DATE_ADD(data_ref, INTERVAL -2 DAY) -- processadas em D-2
			AND A.CodigoSubTipoPromocao = 1
			AND B.nivel2='varejo'
			AND B.nivel3 IN ('supermercado','outros')
)
, addition_telecom AS (
		SELECT			
			DISTINCT A.chavecliente,
			1 AS addition,
                        1 AS telecom
		FROM			
			`project1.dataset_views.table_extrato`	A
			LEFT JOIN `project2.dataset_prod2.table_parceiros` B
  		USING (chaveparceiro,chavefilialparceiro)
		WHERE 0=0
			AND DATE(A.DataProcessamentoTransacao,'America/Sao_Paulo') = DATE_ADD(data_ref, INTERVAL -2 DAY) -- processadas em D-2
			AND B.nivel2='varejo'
			AND B.nivel3='telecom'
)
, addition_bancos AS (
		SELECT			
			DISTINCT A.chavecliente,
			1 AS addition,
                        1 AS bancos
		FROM			
			`project1.dataset_views.table_extrato`	A
			LEFT JOIN `project2.dataset_prod2.table_parceiros` B
  		USING (chaveparceiro,chavefilialparceiro)
		WHERE 0=0
			AND DATE(A.DataProcessamentoTransacao,'America/Sao_Paulo') = DATE_ADD(data_ref, INTERVAL -7 DAY) -- processadas em D-7
			AND B.nivel2='bancos'
			AND B.nivel3 NOT IN ('especiais_1','especiais2') -- projetos especiais
)
, addition_contapontos AS (
		SELECT			
			DISTINCT A.chavecliente,
                        1 AS contapontos,
                        1 AS acumctadigital,
		FROM			
			`project1.dataset_views.table_extrato`	A
			LEFT JOIN `project2.dataset_prod2.table_parceiros` B
  		USING (chaveparceiro,chavefilialparceiro)
		WHERE 0=0
			AND DATE(A.DataProcessamentoTransacao,'America/Sao_Paulo') = DATE_ADD(data_ref, INTERVAL -3 DAY) -- processadas em D-3
			AND B.nivel2='contapontos'
)
, exchange_trip AS (
		SELECT			
			DISTINCT A.chavecliente,
			1 AS exchange,
                        1 AS trip
		FROM			
			`project1.dataset_views.table_exchange`	A
			LEFT JOIN `project2.dataset_prod2.table_classe_exchange` C
  		USING (chavetable_exchange)
		WHERE 0=0
			AND DATE(A.DataSolicitacaoexchange,'America/Sao_Paulo') = DATE_ADD(data_ref, INTERVAL -3 DAY) -- processadas em D-3
                        AND A.FlagexchangeValida = 'S'
			AND C.DescricaoOrigemexchange='trippontos'
)
, exchange_physicalchannel AS (
		SELECT			
			DISTINCT A.chavecliente,
			1 AS exchange,
                        1 AS physicalchannel
		FROM			
			`project1.dataset_views.table_exchange`	A
			LEFT JOIN `project2.dataset_prod2.table_classe_exchange` C
  		USING (chavetable_exchange)
		WHERE 0=0
			AND DATE(A.DataSolicitacaoexchange,'America/Sao_Paulo') = DATE_ADD(data_ref, INTERVAL -2 DAY) -- processadas em D-2
                        AND A.FlagexchangeValida = 'S'
			AND C.DescricaoOrigemexchange IN ("physicalchannel", "tef")
)
, exchange_digitalchannel AS (
		SELECT			
			DISTINCT A.chavecliente,
			1 AS exchange,
                        1 AS digitalchannel
		FROM			
			`project1.dataset_views.table_exchange`	A
			JOIN `project1.dataset_views.table_status_exchange` B 
			USING (ChaveStatusexchange)
			LEFT JOIN `project2.dataset_prod2.table_classe_exchange` C
  		USING (chavetable_exchange)
		WHERE 0=0
			AND ( 
				(DATE(A.DataSolicitacaoexchange,'America/Sao_Paulo') = DATE_ADD(data_ref, INTERVAL -1 DAY)) -- exchange solicitada
				OR
				(B.NomeStatusexchange  = 'OK - Produto Entregue' AND DATE(A.DataSolicitacaoexchange,'America/Sao_Paulo') 
					BETWEEN DATE_ADD(data_ref, INTERVAL -15 DAY) AND DATE_ADD(data_ref, INTERVAL -7 DAY)) -- exchange entregue
					) 
                        AND A.FlagexchangeValida = 'S'
			AND C.DescricaoOrigemexchange IN ("mobile","site")
)
, exchange_money AS (
		SELECT			
			DISTINCT A.chavecliente,
			1 AS exchange,
                        1 AS money
		FROM			
			`project1.dataset_views.table_exchange`	A
			LEFT JOIN `project2.dataset_prod2.table_classe_exchange` C
  		USING (chavetable_exchange)
		WHERE 0=0
			AND DATE(A.DataSolicitacaoexchange,'America/Sao_Paulo') = DATE_ADD(data_ref, INTERVAL -3 DAY) -- processadas em D-3
                        AND A.FlagexchangeValida = 'S'
			AND C.DescricaoOrigemexchange='contapontos'
)
, pix_sent AS (
  SELECT
	  DISTINCT D.chavecliente,
		1 AS pix,
		1 AS contapontos
          FROM
		`project2.dataset_pix.table_transactions` A
	  INNER JOIN `project1.dataset_views.table_customers_a` D
		ON ( LPAD(A.debitPartyTaxId, 11, '0') = LPAD(D.NumeroDocumento, 11, '0') )
	  WHERE 0=0
	    AND A.status = 'CONFIRMED'
	    AND A.debitPartyPersonType = 'F'
	    AND DATE(A.createTimestamp,"America/Sao_Paulo") = DATE_ADD(data_ref, INTERVAL -3 DAY) -- processadas em D-3
)
, pix_received AS (
  SELECT
	  DISTINCT D.chavecliente,
		1 AS pix,
		1 AS contapontos
          FROM
	  `project2.dataset_pix.table_transactions` A
		INNER JOIN `project1.dataset_views.table_customers_a` D
		ON ( LPAD(A.creditPartyTaxId, 11, '0') = LPAD(D.NumeroDocumento, 11, '0') )  
	  WHERE 0=0
	    AND A.status = 'CONFIRMED'
	    AND A.creditPartyPersonType = 'F'
	    AND DATE(A.createTimestamp,"America/Sao_Paulo") = DATE_ADD(data_ref, INTERVAL -3 DAY) -- processadas em D-3
)
, bondcard AS (
  SELECT 
	  DISTINCT E.chavecliente,
		1 AS cartaobond,
		1 AS contapontos
  FROM `project2.dataset_prod2.table_digital_account` A
   INNER JOIN `project2.dataset_prod2.table_digital_account_docs` B
   ON A.ID_DOC = B.id_doc
   INNER JOIN `project2.dataset_digital_account.table_digital_account_type` C
   ON A.ID_CONTA = C.ID_CONTA
   INNER JOIN `project2.dataset_digital_account.table_digital_account_person` D
   ON C.ID_PESSOAFISICA = D.ID_PESSOAFISICA
   INNER JOIN `project1.dataset_views.table_customers_a` E
	 ON ( LPAD(CAST(D.NU_CPF AS STRING), 11, '0') = LPAD(E.NumeroDocumento, 11, '0') ) 
  WHERE 0=0
    AND B.natureza_transacao = 'Compra'
    AND DATE(A.DT_TRANSACAO_TIMESTAMP,'America/Sao_Paulo') = DATE_ADD(data_ref, INTERVAL -3 DAY) -- processadas em D-3
)
, CONTApontos AS (
	SELECT * 
  FROM addition_contapontos
  FULL OUTER JOIN pix_sent USING (chavecliente,contapontos)
  FULL OUTER JOIN pix_received USING (chavecliente,contapontos,pix)
  FULL OUTER JOIN bondcard USING (chavecliente,contapontos)
)
, BASE_AQUISICAO_AUX AS (
  SELECT 
    b.document AS NU_CPF,
    d.chavecliente,
    SPLIT(b.name, ' ')[OFFSET(0)] AS name,
    b.email,
    b.mobile_number as celular,
    DATE_DIFF(CURRENT_DATE('America/Sao_Paulo'), DATE(b.birthdate), DAY) AS dias,
    b.address_city AS cidade,
    'contrato ativo' AS status_do_contrato,
    b.address_state AS uf,
    
    CASE 
      WHEN l.flow = 'fgts_flow' then 'FGTS'
      WHEN l.flow = 'default_flow' then 'Cliente Novo'
      WHEN l.flow = 'pre_approved_flow' then 'Pre aprovado'
      WHEN l.flow = 'recurring_flow' then 'Recorrente'
      ELSE 'exchangeeGanhe'
      END AS tipo_de_cliente,
    
    CASE 
      WHEN l.flow = 'refinancing_flow' THEN  'positivo'
      WHEN l.partner_id = 3 THEN 'credpartner3'
      WHEN l.partner_id = 4 THEN 'credpartner4'
      WHEN l.partner_id = 5 THEN 'credpartner5'
      WHEN JSON_EXTRACT_SCALAR(l.utm_data, '$.utm_source') IS NULL AND l.flow = 'pre_approved_flow' THEN 'pontos'
      WHEN l.partner_id = 1 AND JSON_EXTRACT_SCALAR(l.utm_data, '$.utm_campaign') = 'finanzero' THEN 'credpartner1'                
      WHEN l.partner_id = 1 AND JSON_EXTRACT_SCALAR(l.utm_data, '$.utm_source') IN ('pontos','responsys') THEN 'credpartner2'
      WHEN l.partner_id = 1 AND l.utm_data is not null THEN 'Outros'
      ELSE 'positivo'
      END as origem
  
  FROM `project2.positivo.table_borrowers` b 
  LEFT JOIN `project2.dataset_prod3.table_loans` l 
    ON b.id = l.borrower_id 
  
  LEFT JOIN `project2.dataset_prod3.table_partners` p 
    ON l.partner_id = p.id

  LEFT JOIN `project1.dataset_views.table_customers_a` d
    ON ( LPAD(b.document, 11, '0') = LPAD(d.NumeroDocumento, 11, '0') )
  
  WHERE 0=0
    AND l.state = 'lent'
    AND DATE(l.lent_at,'America/Sao_Paulo') = DATE_ADD(data_ref, INTERVAL -3 DAY) -- processadas em D-3
)
, BASE_QUITADOS_AUX1 as (
  SELECT 
    b.document AS NU_CPF,
    SPLIT(b.name, ' ')[OFFSET(0)] AS name,
    b.email,
    b.mobile_number as celular,
    DATE_DIFF(CURRENT_DATE('America/Sao_Paulo'), DATE(b.birthdate), DAY) AS dias,
    b.address_city as cidade,
    'contrato ativo' as status_do_contrato,
    b.address_state as uf,
    
    CASE 
      WHEN l.flow = 'fgts_flow' then 'FGTS'
      WHEN l.flow = 'default_flow' then 'Cliente Novo'
      WHEN l.flow = 'pre_approved_flow' then 'Pre aprovado'
      WHEN l.flow = 'recurring_flow' then 'Recorrente'
      ELSE 'exchangeeGanhe'
      END AS tipo_de_cliente,
    
    CASE 
      WHEN l.flow = 'refinancing_flow' THEN  'positivo'
      WHEN l.partner_id = 3 THEN 'credpartner3'
      WHEN l.partner_id = 4 THEN 'credpartner4'
      WHEN l.partner_id = 5 THEN 'credpartner5'
      WHEN JSON_EXTRACT_SCALAR(l.utm_data, '$.utm_source') IS NULL AND l.flow = 'pre_approved_flow' THEN 'pontos'
      WHEN l.partner_id = 1 AND JSON_EXTRACT_SCALAR(l.utm_data, '$.utm_campaign') = 'finanzero' THEN 'credpartner1'                
      WHEN l.partner_id = 1 AND JSON_EXTRACT_SCALAR(l.utm_data, '$.utm_source') IN ('pontos','responsys') THEN 'credpartner2'
      WHEN l.partner_id = 1 AND l.utm_data is not null THEN 'Outros'
      ELSE 'positivo'
      END as origem,

    l.id AS contrato,
    COUNT(late_at) as qnt_parcelas_atrasadas
  
  FROM `project2.dataset_prod3.table_borrowers` b 
  LEFT JOIN `project2.dataset_prod3.table_loans` l 
    ON b.id = l.borrower_id 
  
  LEFT JOIN `project2.dataset_prod3.table_partners` p 
    ON l.partner_id = p.id
  
  LEFT JOIN `project2.dataset_prod3.table_parcelas` i
    ON l.id = i.loan_id
  
  WHERE 0=0
    AND l.state = 'settled'
    AND DATE(l.settled_at,'America/Sao_Paulo') = DATE_ADD(data_ref, INTERVAL -3 DAY) -- processadas em D-3
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
, BASE_QUITADOS_AUX2 AS (
SELECT
  DISTINCT NU_CPF,
  B.chavecliente,
  name, 
  email, 
  celular, 
  dias, 
  cidade, 
  CASE WHEN qnt_parcelas_atrasadas = 0 THEN 'em_dia' ELSE 'atraso' END AS forma_quitacao, 
  uf, 
  tipo_de_cliente, 
  origem
FROM BASE_QUITADOS_AUX1 A
LEFT JOIN `project1.dataset_views.table_customers_a` B
  ON ( LPAD(A.NU_CPF, 11, '0') = LPAD(B.NumeroDocumento, 11, '0') )
)
, CREDITO_AQUISICAOplus AS (
  SELECT 
    DISTINCT NU_CPF AS cpf,
    chavecliente,
    1 AS aquisicaoplus,
    1 AS credito
  FROM BASE_AQUISICAO_AUX A
  WHERE 0=0
   AND A.origem <> 'pontos'
)
, CREDITO_AQUISICAOpts AS (
  SELECT 
    DISTINCT NU_CPF AS cpf,
    chavecliente,
    1 AS aquisicaopts,
    1 AS credito
  FROM BASE_AQUISICAO_AUX A
  WHERE 0=0
   AND A.origem = 'pontos'
)
, CREDITO_AQUISICAOFGTS AS (
  SELECT 
    DISTINCT NU_CPF AS cpf,
    chavecliente,
    1 AS aquisicaofgts,
    1 AS credito
  FROM BASE_AQUISICAO_AUX A
  WHERE 0=0
   AND A.tipo_de_cliente = 'FGTS'
)
, CREDITO_QUITADOSplus AS (
  SELECT 
    DISTINCT NU_CPF AS cpf,
    chavecliente,
    1 AS quitadosplus,
    1 AS credito
  FROM BASE_QUITADOS_AUX2 A
  WHERE 0=0
   AND A.origem <> 'pontos'
)
, CREDITO_QUITADOSpts AS (
  SELECT 
    DISTINCT NU_CPF AS cpf,
    chavecliente,
    1 AS quitadospts,
    1 AS credito
  FROM BASE_QUITADOS_AUX2 A
  WHERE 0=0
   AND A.origem = 'pontos'
)
, CREDITO AS (
	SELECT * 
	FROM CREDITO_AQUISICAOplus
	FULL OUTER JOIN CREDITO_AQUISICAOpts USING (cpf,chavecliente,credito)
	FULL OUTER JOIN CREDITO_AQUISICAOFGTS USING (cpf,chavecliente,credito)
	FULL OUTER JOIN CREDITO_QUITADOSplus USING (cpf,chavecliente,credito)
	FULL OUTER JOIN CREDITO_QUITADOSpts USING (cpf,chavecliente,credito)
)
, ELEGIVEIS_PRIMARIOS_SEM_CREDITO AS (
  SELECT 
	chavecliente,
	LPAD(L.NumeroDocumento, 11, '0') AS cpf, 
	addition,
	exchange,
	contapontos,
	bonus,
	duplamarca,
	online,
	varejo,
	telecom,
	bancos,
	trip,
	physicalchannel,
	digitalchannel,
	money,
	acumctadigital,
	pix,
	cartaobond
  FROM addition_bonus A
  FULL OUTER JOIN addition_duplamarca B USING (chavecliente,addition)
  FULL OUTER JOIN addition_online C USING (chavecliente,addition)
  FULL OUTER JOIN addition_varejo D USING (chavecliente,addition)
  FULL OUTER JOIN addition_telecom E USING (chavecliente,addition)
  FULL OUTER JOIN addition_bancos F USING (chavecliente,addition)
  FULL OUTER JOIN exchange_trip G USING (chavecliente)
  FULL OUTER JOIN exchange_physicalchannel H USING (chavecliente,exchange)
  FULL OUTER JOIN exchange_digitalchannel I USING (chavecliente,exchange)
  FULL OUTER JOIN exchange_money J USING (chavecliente,exchange)
  FULL OUTER JOIN CONTApontos K USING (chavecliente)
  INNER JOIN `project1.dataset_views.table_customers_a` L USING (chavecliente)
)
, ELEGIVEIS_PRIMARIOS_COM_CREDITO AS (
  SELECT *,
  FROM ELEGIVEIS_PRIMARIOS_SEM_CREDITO
  FULL OUTER JOIN CREDITO USING(cpf,chavecliente)
)
, ELEGIVEIS_PRIMARIOS_SEM_NULOS AS (
  SELECT
	DISTINCT cpf,
    	chavecliente,
    	IFNULL(A.addition,0) AS addition,
	IFNULL(A.exchange,0) AS exchange,
	IFNULL(A.contapontos,0) AS contapontos,
    	IFNULL(A.credito,0) AS credito,
	IFNULL(A.bonus,0) AS bonus,
	IFNULL(A.duplamarca,0) AS duplamarca,
	IFNULL(A.online,0) AS online,
	IFNULL(A.varejo,0) AS varejo,
    	IFNULL(A.telecom,0) AS telecom,
	IFNULL(A.bancos,0) AS bancos,
	IFNULL(A.trip,0) AS trip,
	IFNULL(A.physicalchannel,0) AS physicalchannel,
	IFNULL(A.digitalchannel,0) AS digitalchannel,
	IFNULL(A.money,0) AS money,
    	IFNULL(A.acumctadigital,0) AS acumctadigital,
	IFNULL(A.pix,0) AS pix,
	IFNULL(A.cartaobond,0) AS cartaobond,
    	IFNULL(A.aquisicaoplus,0) AS aquisicaoplus,
    	IFNULL(A.aquisicaopts,0) AS aquisicaopts,
    	IFNULL(A.aquisicaofgts,0) AS aquisicaofgts,
    	IFNULL(A.quitadosplus,0) AS quitadosplus,
    	IFNULL(A.quitadospts,0) AS quitadospts,
    	ROW_NUMBER() OVER(PARTITION BY cpf ORDER BY chavecliente DESC) AS rank
	FROM ELEGIVEIS_PRIMARIOS_COM_CREDITO A
)
SELECT * EXCEPT(rank), data_ref AS data_atualizacao
FROM ELEGIVEIS_PRIMARIOS_SEM_NULOS
WHERE rank=1
)
;

CREATE OR REPLACE TABLE dataset_temp1.table_elegiveis_norestrictions AS (

WITH LISTA_EMAIL_RUIM AS (
  SELECT
    DISTINCT A.chavecliente,CPF AS NU_CPF 
  FROM `project2.dataset_prod2.table_contatos` A
	WHERE 0=0
	  AND (A.FaixaScoreEmail IN ('Ruim','Médio') OR A.FaixaScoreEmail IS NULL)
)
, LISTA_CONTAS_INATIVAS AS (
	SELECT
		DISTINCT chavecliente, LPAD(A.NumeroDocumento, 11, '0') AS NU_CPF
	FROM `project1.dataset_views.table_customers_a` A
	WHERE 0=0
	  AND (A.DescricaoStatusConta <> 'Ativado' OR A.DescricaoStatusConta IS NULL)
)
, LISTA_OPTOUT AS (
  SELECT
	DISTINCT chavecliente, LPAD(CAST(A.CPF AS STRING), 11, '0') AS NU_CPF
  FROM `project2.dataset_gov.table_optout` A
)
, LISTA_LGPD_plus AS (
    SELECT
	DISTINCT A.document AS NU_CPF
    FROM `project2.positivo.table_borrowers` A
    WHERE 0=0
      AND A.lgpd_excluded_data=TRUE
)
, LISTA_PREVENCAO_FRAUDE AS (
  SELECT
    DISTINCT COALESCE(F.NU_CPF, M.NU_CPF) AS NU_CPF
  FROM (
    SELECT
      CASE WHEN Tipo_Perda = 'Conta Digital' THEN Cod_Cliente ELSE NULL END ID_CONTA,
      CASE WHEN Tipo_Perda <> 'Conta Digital' THEN Cod_Cliente ELSE NULL END NU_CPF,
    FROM `project2.dataset_fraude.table_losses` 
) F
  LEFT JOIN (
  SELECT 
    CAST(A.ID_CONTA AS STRING) AS ID_CONTA, 
    LPAD(CAST(B.NU_CPF AS STRING), 11, '0')  AS NU_CPF
  FROM `project2.dataset_digital_account.table_digital_account_type` A
  INNER JOIN `project2.dataset_digital_account.table_digital_account_person` B
    ON A.ID_PESSOAFISICA = B.ID_PESSOAFISICA
) M
USING
  (ID_CONTA)
)
, BASE_QUARENTENA_AUX1 AS (
  SELECT _id,H.COLUMN,H.value
  FROM `project2.dataset_nps.table_coplusites`, UNNEST(indicators) H
 )
,BASE_QUARENTENA_AUX2 AS (
  SELECT 
    _id,
    MAX(CASE WHEN column = 'chavecliente' THEN value ELSE NULL END) AS chavecliente,
    MAX(CASE WHEN column = 'numerocpf' THEN value ELSE NULL END) AS NU_CPF
  FROM BASE_QUARENTENA_AUX1 
  GROUP BY 1
	)
, BASE_QUARENTENA_AUX3 AS (
SELECT 
  A.createdAt,
  CAST(B.chavecliente AS INTEGER) AS chavecliente,
  B.NU_CPF,
  CASE 
    WHEN answered=false THEN DATE_ADD((DATE(createdAt,'America/Sao_Paulo')), INTERVAL 15 DAY) 
    WHEN answered=true THEN DATE_ADD((DATE(createdAt,'America/Sao_Paulo')), INTERVAL 90 DAY)
    ELSE NULL END QuarantineExpiryDate,
FROM `project2.dataset_nps.table_coplusites` A
LEFT JOIN BASE_QUARENTENA_AUX2 B ON A._id = B._id
WHERE B.chavecliente IS NOT NULL
)
, BASE_QUARENTENA_AUX4 AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY chavecliente ORDER BY DATE(QuarantineExpiryDate) DESC) AS RANK
FROM BASE_QUARENTENA_AUX3
)
, LISTA_QUARENTENA AS (
SELECT chavecliente, NU_CPF, QuarantineExpiryDate
FROM BASE_QUARENTENA_AUX4
WHERE 0=0
  AND RANK=1
  AND DATE(QuarantineExpiryDate)>=data_ref
)
, LISTA_FUNCIONARIOS_pts_plus AS (
SELECT LPAD(CAST(A.CPF AS STRING), 11, '0') AS NU_CPF
FROM `project1.dataset_temp1.table_funcionarios` A
WHERE A.CPF IS NOT NULL
)
SELECT A.*
FROM dataset_temp1.table_elegiveis_iniciais A
LEFT JOIN LISTA_EMAIL_RUIM B
ON a.cpf = B.NU_CPF
LEFT JOIN LISTA_CONTAS_INATIVAS C
ON A.cpf = C.NU_CPF
LEFT JOIN LISTA_OPTOUT D
ON A.cpf = D.NU_CPF
 LEFT JOIN LISTA_LGPD_plus E
 ON A.cpf = E.NU_CPF
LEFT JOIN LISTA_PREVENCAO_FRAUDE F
ON A.cpf = F.NU_CPF
LEFT JOIN LISTA_QUARENTENA G
ON A.cpf = G.NU_CPF
LEFT JOIN LISTA_FUNCIONARIOS_pts_plus H
ON A.cpf = H.NU_CPF
WHERE 0=0
	AND B.NU_CPF IS NULL -- não é email inválido
	AND C.NU_CPF IS NULL -- não é conta inativa
	AND D.NU_CPF IS NULL -- sem optout
        AND E.NU_CPF IS NULL -- não está na lista LGPD
	AND F.NU_CPF IS NULL -- não está na lista de prevencao a fraude
	AND G.NU_CPF IS NULL -- não está em quarentena
	AND H.NU_CPF IS NULL -- não é funcionário
)
;

CREATE OR REPLACE TABLE dataset_temp1.NOVO_NPS_positivo AS (
WITH ELEGIVEIS_CAPTURADOS_positivo AS (
SELECT *
FROM dataset_temp1.table_elegiveis_norestrictions
WHERE 0=0
  AND credito = 1
  AND aquisicaopts = 0
  AND quitadospts = 0
  AND RAND() <= 0.05
)
SELECT
  CASE WHEN B.name IS NULL OR TRIM(B.name) = '' THEN 'CLIENTE positivo'
    ELSE UPPER(SPLIT(B.name, ' ')[SAFE_OFFSET(0)]) END nome,
  B.email,
  CAST(B.mobile_number AS INTEGER) as telefone,
  FORMAT_DATE('%d/%m/%Y', data_ref) AS data_atualizacao,
  A.* EXCEPT (data_atualizacao)
FROM ELEGIVEIS_CAPTURADOS_positivo A
INNER JOIN `project2.positivo.table_borrowers` B
ON A.cpf = B.document
)
;

CREATE OR REPLACE TABLE dataset_temp1.NOVO_NPS_pontos AS (

WITH ELEGIVEIS_CAPTURADOS_pontos_PARTE1 AS (
SELECT *
FROM dataset_temp1.table_elegiveis_norestrictions
WHERE 0=0
  AND credito = 0
  AND (bonus=1 OR trip=1)
  AND RAND() <= 0.50
)
, ELEGIVEIS_CAPTURADOS_pontos_PARTE2 AS (
SELECT *
FROM dataset_temp1.table_elegiveis_norestrictions
WHERE 0=0
  AND credito = 0
  AND (bonus=0 AND trip=0)
  AND RAND() <= 0.05
)
, ELEGIVEIS_CAPTURADOS_pontos_PARTE3 AS (
SELECT *
FROM dataset_temp1.table_elegiveis_norestrictions
WHERE 0=0
  AND credito = 1
  AND (aquisicaopts=1 OR quitadospts=1)
  AND RAND() <= 0.05
)
,ELEGIVEIS_CAPTURADOS_pontos_AUX AS (
SELECT * FROM ELEGIVEIS_CAPTURADOS_pontos_PARTE1
UNION ALL
SELECT * FROM ELEGIVEIS_CAPTURADOS_pontos_PARTE2
UNION ALL
SELECT * FROM ELEGIVEIS_CAPTURADOS_pontos_PARTE3
)
, ELEGIVEIS_CAPTURADOS_pontos AS (
SELECT
  CASE WHEN B.DescricaoNome IS NULL OR TRIM(DescricaoNome) = '' THEN 'CLIENTE pontos'
    ELSE UPPER(SPLIT(B.DescricaoNome, ' ')[SAFE_OFFSET(0)]) END nome,
  C.EMAIL AS email,
  B.NumeroTelefone AS telefone,
  ROW_NUMBER() OVER(PARTITION BY LPAD(B.NumeroDocumento, 11, '0') ORDER BY B.DataUltimaAlteracaoCadastro DESC) AS rank,
  FORMAT_DATE('%d/%m/%Y', data_ref) AS data_atualizacao,
  A.* EXCEPT (data_atualizacao)
FROM ELEGIVEIS_CAPTURADOS_pontos_AUX A
  INNER JOIN `project1.dataset_views.table_customers_a` B ON A.cpf = LPAD(B.NumeroDocumento, 11, '0')
  INNER JOIN `project2.dataset_prod2.table_contatos` C ON A.cpf = C.CPF
)
SELECT * EXCEPT(rank)
FROM ELEGIVEIS_CAPTURADOS_pontos
WHERE rank=1
)
;
