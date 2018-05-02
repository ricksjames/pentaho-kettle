package org.pentaho.di.trans.step.amqp;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStreamingDialog;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Arrays.stream;

public class AmqpConsumerDialog extends BaseStreamingDialog implements StepDialogInterface {

  private static Class<?> PKG = AmqpConsumerMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private AmqpConsumerMeta streamMeta;

  private TextVar wHostname;

  private Combo wExchangeType;
  private Label wlExchangeName;
  private TextVar wExchangeName;
  private Button wExchangeDurable;
  private Button wExchangeExclusive;
  private Button wExchangeAutoDelete;

  private TextVar wQueueName;
  private Label wlQueueName;
  private Button wQueueDurable;
  private Button wQueueExclusive;
  private Button wQueueAutoDelete;

  private TableView routingKeyTable;
  private Label wlRoutingKey;
  private TableView fieldsTable;

  public AmqpConsumerDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, in, tr, sname );
    streamMeta = (AmqpConsumerMeta) in;
  }

  @Override protected String getDialogTitle() {
    return BaseMessages.getString( PKG, "AmqpDialog.Shell.Title" );
  }

  @Override protected void buildSetup( Composite wSetupComp ) {

    //Hostname Label
    Label wlHostname = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlHostname );
    wlHostname.setText( BaseMessages.getString( PKG, "AmqpDialog.Hostname.Label" ) );
    FormData fdlHostname = new FormData();
    fdlHostname.left = new FormAttachment( 0, 0 );
    fdlHostname.top = new FormAttachment( 0, 0 );
    fdlHostname.right = new FormAttachment( 50, 0 );
    wlHostname.setLayoutData( fdlHostname );

    //Hostname
    wHostname = new TextVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wHostname );
    wHostname.addModifyListener( lsMod );
    FormData fdHostname = new FormData();
    fdHostname.left = new FormAttachment( 0, 0 );
    fdHostname.top = new FormAttachment( wlHostname, 5 );
    fdHostname.right = new FormAttachment( 0, INPUT_WIDTH );
    wHostname.setLayoutData( fdHostname );

    //Exchange Type Label
    Label wlExchangeType = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlExchangeType );
    wlExchangeType.setText( BaseMessages.getString( PKG, "AMQPDialog.ExchangeType.Label" ) );
    FormData fdlExchangeType = new FormData();
    fdlExchangeType.left = new FormAttachment( 0, 0 );
    fdlExchangeType.top = new FormAttachment( wHostname, 10 );
    fdlExchangeType.right = new FormAttachment( 0, INPUT_WIDTH );
    wlExchangeType.setLayoutData( fdlExchangeType );

    //Exchange Type
    wExchangeType = new Combo( wSetupComp, SWT.READ_ONLY | SWT.LEFT );
    props.setLook( wExchangeType );
    wExchangeType.addModifyListener( lsMod );
    FormData fdExchangeType = new FormData();
    fdExchangeType.left = new FormAttachment( 0, 0 );
    fdExchangeType.top = new FormAttachment( wlExchangeType, 10 );
    fdExchangeType.width = 135;
    wExchangeType.setLayoutData( fdExchangeType );
    wExchangeType.add( "default" );
    wExchangeType.add( "fanout" );
    wExchangeType.add( "direct" );
    wExchangeType.add( "topic" );
    wExchangeType.add( "headers" );

    wExchangeType.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        exchangeChanged();
      }
    } );

    //Exchange Name Label
    wlExchangeName = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlExchangeName );
    wlExchangeName.setText( BaseMessages.getString( PKG, "AmqpDialog.ExchangeName.Label" ) );
    FormData fdlExchangeName = new FormData();
    fdlExchangeName.left = new FormAttachment( 0, 0 );
    fdlExchangeName.top = new FormAttachment( wExchangeType, 10 );
    fdlExchangeName.right = new FormAttachment( 50, 0 );
    wlExchangeName.setLayoutData( fdlExchangeName );

    //Exchange Name
    wExchangeName = new TextVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wExchangeName );
    wExchangeName.addModifyListener( lsMod );
    FormData fdExchangeName = new FormData();
    fdExchangeName.left = new FormAttachment( 0, 0 );
    fdExchangeName.top = new FormAttachment( wlExchangeName, 5 );
    fdExchangeName.right = new FormAttachment( 0, INPUT_WIDTH );
    wExchangeName.setLayoutData( fdExchangeName );

    //Exchange Durable
    wExchangeDurable = new Button( wSetupComp, SWT.CHECK );
    wExchangeDurable.setText( BaseMessages.getString( PKG, "AmqpDialog.Durable.Label" ) );
    props.setLook( wExchangeDurable );
    FormData fdExchangeDurable = new FormData();
    fdExchangeDurable.top = new FormAttachment( wExchangeName, 5 );
    fdExchangeDurable.left = new FormAttachment( 0, 0 );
    wExchangeDurable.setLayoutData( fdExchangeDurable );

    //Exchange Exclusive
    wExchangeExclusive = new Button( wSetupComp, SWT.CHECK );
    wExchangeExclusive.setText( BaseMessages.getString( PKG, "AmqpDialog.Exclusive.Label" ) );
    props.setLook( wExchangeExclusive );
    FormData fdExchangeExclusive = new FormData();
    fdExchangeExclusive.top = new FormAttachment( wExchangeName, 5 );
    fdExchangeExclusive.left = new FormAttachment( wExchangeDurable, 15 );
    wExchangeExclusive.setLayoutData( fdExchangeExclusive );

    //Exchange AutoDelete
    wExchangeAutoDelete = new Button( wSetupComp, SWT.CHECK );
    wExchangeAutoDelete.setText( BaseMessages.getString( PKG, "AmqpDialog.AutoDelete.Label" ) );
    props.setLook( wExchangeAutoDelete );
    FormData fdExchangeAutoDelete = new FormData();
    fdExchangeAutoDelete.top = new FormAttachment( wExchangeName, 5 );
    fdExchangeAutoDelete.left = new FormAttachment( wExchangeExclusive, 15 );
    wExchangeAutoDelete.setLayoutData( fdExchangeAutoDelete );

    //Queue Name Label
    wlQueueName = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlQueueName );
    wlQueueName.setText( BaseMessages.getString( PKG, "AmqpDialog.QueueName.Label" ) );
    FormData fdlQueueName = new FormData();
    fdlQueueName.left = new FormAttachment( 0, 0 );
    fdlQueueName.top = new FormAttachment( wExchangeDurable, 10 );
    fdlQueueName.right = new FormAttachment( 50, 0 );
    wlQueueName.setLayoutData( fdlQueueName );

    //Queue Name
    wQueueName = new TextVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wQueueName );
    wQueueName.addModifyListener( lsMod );
    FormData fdQueueName = new FormData();
    fdQueueName.left = new FormAttachment( 0, 0 );
    fdQueueName.top = new FormAttachment( wlQueueName, 5 );
    fdQueueName.right = new FormAttachment( 0, INPUT_WIDTH );
    wQueueName.setLayoutData( fdQueueName );

    //Queue Durable
    wQueueDurable = new Button( wSetupComp, SWT.CHECK );
    wQueueDurable.setText( BaseMessages.getString( PKG, "AmqpDialog.Durable.Label" ) );
    props.setLook( wQueueDurable );
    FormData fdQueueDurable = new FormData();
    fdQueueDurable.top = new FormAttachment( wQueueName, 5 );
    fdQueueDurable.left = new FormAttachment( 0, 0 );
    wQueueDurable.setLayoutData( fdQueueDurable );

    //Queue Exclusive
    wQueueExclusive = new Button( wSetupComp, SWT.CHECK );
    wQueueExclusive.setText( BaseMessages.getString( PKG, "AmqpDialog.Exclusive.Label" ) );
    props.setLook( wQueueExclusive );
    FormData fdQueueExclusive = new FormData();
    fdQueueExclusive.top = new FormAttachment( wQueueName, 5 );
    fdQueueExclusive.left = new FormAttachment( wQueueDurable, 15 );
    wQueueExclusive.setLayoutData( fdQueueExclusive );

    //Queue AutoDelete
    wQueueAutoDelete = new Button( wSetupComp, SWT.CHECK );
    wQueueAutoDelete.setText( BaseMessages.getString( PKG, "AmqpDialog.AutoDelete.Label" ) );
    props.setLook( wQueueAutoDelete );
    FormData fdQueueAutoDelete = new FormData();
    fdQueueAutoDelete.top = new FormAttachment( wQueueName, 5 );
    fdQueueAutoDelete.left = new FormAttachment( wQueueExclusive, 15 );
    wQueueAutoDelete.setLayoutData( fdQueueAutoDelete );

    //TODO: INSERT HEADER ARGS HERE

    //Routing Key Label
    wlRoutingKey = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlRoutingKey );
    wlRoutingKey.setText( BaseMessages.getString( PKG, "AmqpDialog.RoutingKey.Label" ) );
    FormData fdlRoutingKey = new FormData();
    fdlRoutingKey.left = new FormAttachment( 0, 0 );
    fdlRoutingKey.top = new FormAttachment( wQueueDurable, 10 );
    fdlRoutingKey.right = new FormAttachment( 50, 0 );
    wlRoutingKey.setLayoutData( fdlRoutingKey );

    //Routing Keys
    buildRoutingKeysTable( wSetupComp, wlRoutingKey );


  }

  private void buildRoutingKeysTable( Composite parentWidget, Control controlAbove/*, Control controlBelow*/ ) {
    ColumnInfo[] columns =
      new ColumnInfo[] { new ColumnInfo( BaseMessages.getString( PKG, "AmqpDialog.RoutingKeys.Label" ),
        ColumnInfo.COLUMN_TYPE_TEXT, new String[ 1 ], false ) };

    columns[ 0 ].setUsingVariables( true );

    int topicsCount = streamMeta.getRoutingKeys().size();

    routingKeyTable = new TableView(
      transMeta,
      parentWidget,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
      columns,
      topicsCount,
      false,
      lsMod,
      props,
      false
    );

    routingKeyTable.setSortable( false );
    routingKeyTable.getTable().addListener( SWT.Resize, event -> {
      Table table = (Table) event.widget;
      table.getColumn( 1 ).setWidth( 330 );
    } );

    FormData fdData = new FormData();
    fdData.left = new FormAttachment( 0, 0 );
    fdData.top = new FormAttachment( controlAbove, 5 );
    fdData.right = new FormAttachment( 0, 350 );
    fdData.bottom = new FormAttachment( 100, 0 );
    //fdData.bottom = new FormAttachment( controlBelow, -10 );

    // resize the columns to fit the data in them
    stream( routingKeyTable.getTable().getColumns() ).forEach( column -> {
      if ( column.getWidth() > 0 ) {
        // don't pack anything with a 0 width, it will resize it to make it visible (like the index column)
        column.setWidth( 120 );
      }
    } );

    routingKeyTable.setLayoutData( fdData );
  }

  private void exchangeChanged() {

    boolean defaultSelected = "default".equals( wExchangeType.getText() );

    wlExchangeName.setEnabled( !defaultSelected );
    wExchangeName.setEnabled( !defaultSelected );
    wExchangeDurable.setEnabled( !defaultSelected );
    wExchangeExclusive.setEnabled( !defaultSelected );
    wExchangeAutoDelete.setEnabled( !defaultSelected );
    wlRoutingKey.setEnabled( !defaultSelected );
    routingKeyTable.setEnabled( !defaultSelected );
  }

  @Override protected void additionalOks( BaseStreamStepMeta meta ) {

    streamMeta.setMsgOutputName( fieldsTable.getTable().getItem( 0 ).getText( 2 ) );
    streamMeta.setQueueOutputName( fieldsTable.getTable().getItem( 1 ).getText( 2 ) );
    streamMeta.setRoutingKeyOutputName( fieldsTable.getTable().getItem( 2 ).getText( 2 ) );
    streamMeta.setExchangeOutputName( fieldsTable.getTable().getItem( 3 ).getText( 2 ) );

    streamMeta.setHostname( wHostname.getText() );

    streamMeta.setExchangeType( wExchangeType.getText() );
    streamMeta.setExchange( wExchangeName.getText() );
    streamMeta.setExchangeDurable( wExchangeDurable.getSelection() );
    streamMeta.setExchangeExclusive( wExchangeExclusive.getSelection() );
    streamMeta.setExchangeAutoDelete( wExchangeAutoDelete.getSelection() );

    streamMeta.setQueue( wQueueName.getText() );
    streamMeta.setQueueDurable( wQueueDurable.getSelection() );
    streamMeta.setQueueExclusive( wQueueExclusive.getSelection() );
    streamMeta.setQueueAutoDelete( wQueueAutoDelete.getSelection() );

    streamMeta.setRoutingKeys( stream( routingKeyTable.getTable().getItems() )
      .map( item -> item.getText( 1 ) )
      .filter( t -> !"".equals( t ) )
      .distinct()
      .collect( Collectors.toList() ) );
  }

  @Override protected void createAdditionalTabs() {
    shell.setMinimumSize( 527, 600 );
    buildFieldsTab();
  }

  private void buildFieldsTab() {
    CTabItem wFieldsTab = new CTabItem( wTabFolder, SWT.NONE, 3 );
    wFieldsTab.setText( BaseMessages.getString( PKG, "AmqpDialog.FieldsTab" ) );

    Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFieldsComp );
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginHeight = 15;
    fieldsLayout.marginWidth = 15;
    wFieldsComp.setLayout( fieldsLayout );

    FormData fieldsFormData = new FormData();
    fieldsFormData.left = new FormAttachment( 0, 0 );
    fieldsFormData.top = new FormAttachment( wFieldsComp, 0 );
    fieldsFormData.right = new FormAttachment( 100, 0 );
    fieldsFormData.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fieldsFormData );

    buildFieldTable( wFieldsComp, wFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );
  }

  private void buildFieldTable( Composite parentWidget, Control relativePosition ) {
    ColumnInfo[] columns = getFieldColumns();

    fieldsTable = new TableView(
      transMeta,
      parentWidget,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
      columns,
      4,
      true,
      lsMod,
      props,
      false
    );

    fieldsTable.setSortable( false );
    fieldsTable.getTable().addListener( SWT.Resize, event -> {
      Table table = (Table) event.widget;
      table.getColumn( 1 ).setWidth( 147 );
      table.getColumn( 2 ).setWidth( 147 );
      table.getColumn( 3 ).setWidth( 147 );
    } );

    populateFieldData();

    FormData fdData = new FormData();
    fdData.left = new FormAttachment( 0, 0 );
    fdData.top = new FormAttachment( relativePosition, 5 );
    fdData.right = new FormAttachment( 100, 0 );

    // resize the columns to fit the data in them
    stream( fieldsTable.getTable().getColumns() ).forEach( column -> {
      if ( column.getWidth() > 0 ) {
        // don't pack anything with a 0 width, it will resize it to make it visible (like the index column)
        column.setWidth( 120 );
      }
    } );

    // don't let any rows get deleted or added (this does not affect the read-only state of the cells)
    fieldsTable.setReadonly( true );
    fieldsTable.setLayoutData( fdData );
  }

  private void populateFieldData() {
    TableItem messageItem = fieldsTable.getTable().getItem( 0 );
    messageItem.setText( 1, BaseMessages.getString( PKG, "AmqpDialog.InputName.Message" ) );
    messageItem.setText( 2, streamMeta.getMsgOutputName() );
    messageItem.setText( 3, "String" );

    TableItem topicItem = fieldsTable.getTable().getItem( 1 );
    topicItem.setText( 1, BaseMessages.getString( PKG, "AmqpDialog.InputName.Queue" ) );
    topicItem.setText( 2, streamMeta.getQueueOutputName() );
    topicItem.setText( 3, "String" );

    TableItem routingKeyItem = fieldsTable.getTable().getItem( 2 );
    routingKeyItem.setText( 1, BaseMessages.getString( PKG, "AmqpDialog.InputName.RoutingKey" ) );
    routingKeyItem.setText( 2, streamMeta.getRoutingKeyOutputName() );
    routingKeyItem.setText( 3, "String" );

    TableItem exchangeItem = fieldsTable.getTable().getItem( 3 );
    exchangeItem.setText( 1, BaseMessages.getString( PKG, "AmqpDialog.InputName.Exchange" ) );
    exchangeItem.setText( 2, streamMeta.getExchangeOutputName() );
    exchangeItem.setText( 3, "String" );
  }

  private ColumnInfo[] getFieldColumns() {
    ColumnInfo referenceName = new ColumnInfo( BaseMessages.getString( PKG, "AmqpDialog.Column.Ref" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, true );

    ColumnInfo name = new ColumnInfo( BaseMessages.getString( PKG, "AmqpDialog.Column.Name" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, false );

    ColumnInfo type = new ColumnInfo( BaseMessages.getString( PKG, "AmqpDialog.Column.Type" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, true );

    return new ColumnInfo[] { referenceName, name, type };
  }

  @Override protected int[] getFieldTypes() {
    return stream( fieldsTable.getTable().getItems() )
      .mapToInt( row -> ValueMetaFactory.getIdForValueMeta( row.getText( 3 ) ) ).toArray();
  }

  @Override protected String[] getFieldNames() {
    return stream( fieldsTable.getTable().getItems() ).map( row -> row.getText( 2 ) ).toArray( String[]::new );
  }

  @Override protected void getData() {
    super.getData();
    wHostname.setText( nullToEmpty( streamMeta.getHostname() ) );

    wExchangeType.setText( nullToEmpty( streamMeta.getExchangeType() ) );
    wExchangeName.setText( nullToEmpty( streamMeta.getExchange() ) );
    wExchangeDurable.setSelection( streamMeta.isExchangeDurable() );
    wExchangeExclusive.setSelection( streamMeta.isExchangeExclusive() );
    wExchangeAutoDelete.setSelection( streamMeta.isExchangeAutoDelete() );

    wQueueName.setText( nullToEmpty( streamMeta.getQueue() ) );
    wQueueDurable.setSelection( streamMeta.isQueueDurable() );
    wQueueExclusive.setSelection( streamMeta.isQueueExclusive() );
    wQueueAutoDelete.setSelection( streamMeta.isQueueAutoDelete() );

    populateRoutingKeyData();

    exchangeChanged();

  }

  private void populateRoutingKeyData() {
    List<String> routingKeys = streamMeta.getRoutingKeys();
    int rowIndex = 0;
    for ( String routingKey : routingKeys ) {
      TableItem key = routingKeyTable.getTable().getItem( rowIndex++ );
      if ( routingKey != null ) {
        key.setText( 1, routingKey );
      }
    }
  }

}
