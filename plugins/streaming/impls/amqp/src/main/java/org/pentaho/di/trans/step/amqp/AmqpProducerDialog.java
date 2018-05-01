package org.pentaho.di.trans.step.amqp;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.List;

import static com.google.common.base.Strings.nullToEmpty;

public class AmqpProducerDialog extends BaseStepDialog implements StepDialogInterface {

  private static final int INPUT_WIDTH = 350;
  private static final int SHELL_MIN_WIDTH = 527;
  private static final int SHELL_MIN_HEIGHT = 650;

  private static Class<?> PKG = AmqpProducerDialog.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private AmqpProducerMeta meta;
  private ModifyListener lsMod;

  private CTabFolder wTabFolder;

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

  private ComboVar wRoutingKey;
  private Label wlRoutingKey;
  private ComboVar wMessageField;

  public AmqpProducerDialog( Shell parent, Object in, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta) in, transMeta, stepname );
    meta = (AmqpProducerMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();
    changed = meta.hasChanged();

    lsMod = e -> meta.setChanged();
    lsCancel = e -> cancel();
    lsOK = e -> ok();
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    setShellImage( shell, meta );
    shell.setMinimumSize( SHELL_MIN_WIDTH, SHELL_MIN_HEIGHT );
    shell.setText( BaseMessages.getString( PKG, "AmqpDialog.Shell.Title" ) );

    Label wicon = new Label( shell, SWT.RIGHT );
    wicon.setImage( getImage() );
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment( 0, 0 );
    fdlicon.right = new FormAttachment( 100, 0 );
    wicon.setLayoutData( fdlicon );
    props.setLook( wicon );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;
    shell.setLayout( formLayout );
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "AMQPProducerDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.top = new FormAttachment( 0, 0 );
    wlStepname.setLayoutData( fdlStepname );

    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.width = 250;
    fdStepname.left = new FormAttachment( 0, 0 );
    fdStepname.top = new FormAttachment( wlStepname, 5 );
    wStepname.setLayoutData( fdStepname );
    wStepname.addSelectionListener( lsDef );

    Label topSeparator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdSpacer = new FormData();
    fdSpacer.height = 2;
    fdSpacer.left = new FormAttachment( 0, 0 );
    fdSpacer.top = new FormAttachment( wStepname, 15 );
    fdSpacer.right = new FormAttachment( 100, 0 );
    topSeparator.setLayoutData( fdSpacer );

    // Start of tabbed display
    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );
    wTabFolder.setUnselectedCloseVisible( true );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    FormData fdCancel = new FormData();
    fdCancel.right = new FormAttachment( 100, 0 );
    fdCancel.bottom = new FormAttachment( 100, 0 );
    wCancel.setLayoutData( fdCancel );
    wCancel.addListener( SWT.Selection, lsCancel );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fdOk = new FormData();
    fdOk.right = new FormAttachment( wCancel, -5 );
    fdOk.bottom = new FormAttachment( 100, 0 );
    wOK.setLayoutData( fdOk );
    wOK.addListener( SWT.Selection, lsOK );

    Label bottomSeparator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    props.setLook( bottomSeparator );
    FormData fdBottomSeparator = new FormData();
    fdBottomSeparator.height = 2;
    fdBottomSeparator.left = new FormAttachment( 0, 0 );
    fdBottomSeparator.bottom = new FormAttachment( wCancel, -15 );
    fdBottomSeparator.right = new FormAttachment( 100, 0 );
    bottomSeparator.setLayoutData( fdBottomSeparator );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( topSeparator, 15 );
    fdTabFolder.bottom = new FormAttachment( bottomSeparator, -15 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    wTabFolder.setLayoutData( fdTabFolder );

    buildSetupTab();

    getData();

    setSize();

    meta.setChanged( changed );

    wTabFolder.setSelection( 0 );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return stepname;
  }


  protected void buildSetupTab() {

    CTabItem wSetupTab = new CTabItem( wTabFolder, SWT.NONE );
    wSetupTab.setText( BaseMessages.getString( PKG, "AmqpDialog.SetupTab" ) );

    Composite wSetupComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wSetupComp );
    FormLayout setupLayout = new FormLayout();
    setupLayout.marginHeight = 15;
    setupLayout.marginWidth = 15;
    wSetupComp.setLayout( setupLayout );

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
    props.setLook(wHostname);
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



    wExchangeType.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected(SelectionEvent e) {
        exchangeChanged();
      }
    });

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

    //Routing Key
    wRoutingKey = new ComboVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRoutingKey );
    wRoutingKey.addModifyListener( lsMod );
    FormData fdRoutingKey = new FormData();
    fdRoutingKey.left = new FormAttachment( 0, 0 );
    fdRoutingKey.top = new FormAttachment( wlRoutingKey, 5 );
    fdRoutingKey.right = new FormAttachment( 0, INPUT_WIDTH );
    wRoutingKey.setLayoutData( fdRoutingKey );
    Listener lsRoutingKeyFocus = e -> {
      String current = wRoutingKey.getText();
      wRoutingKey.getCComboWidget().removeAll();
      wRoutingKey.setText( current );
      try {
        RowMetaInterface rmi = transMeta.getPrevStepFields( meta.getParentStepMeta().getName() );
        List ls = rmi.getValueMetaList();
        for ( Object l : ls ) {
          ValueMetaBase vmb = (ValueMetaBase) l;
          wRoutingKey.add( vmb.getName() );
        }
      } catch ( KettleStepException ex ) {
        // do nothing
      }
    };
    wRoutingKey.getCComboWidget().addListener( SWT.FocusIn, lsRoutingKeyFocus );

    //Message Field Label
    Label wlMessageField = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlMessageField );
    wlMessageField.setText( BaseMessages.getString( PKG, "AmqpDialog.MessageField.Label" ) );
    FormData fdlMessageField = new FormData();
    fdlMessageField.left = new FormAttachment( 0, 0 );
    fdlMessageField.top = new FormAttachment( wRoutingKey, 10 );
    fdlMessageField.right = new FormAttachment( 50, 0 );
    wlMessageField.setLayoutData( fdlMessageField );

    //Message Field
    wMessageField = new ComboVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMessageField );
    wMessageField.addModifyListener( lsMod );
    FormData fdMessageField = new FormData();
    fdMessageField.left = new FormAttachment( 0, 0 );
    fdMessageField.top = new FormAttachment( wlMessageField, 5 );
    fdMessageField.right = new FormAttachment( 0, INPUT_WIDTH );
    wMessageField.setLayoutData( fdMessageField );
    Listener lsMessageFocus = e -> {
      String current = wMessageField.getText();
      wMessageField.getCComboWidget().removeAll();
      wMessageField.setText( current );
      try {
        RowMetaInterface rmi = transMeta.getPrevStepFields( meta.getParentStepMeta().getName() );
        List ls = rmi.getValueMetaList();
        for ( Object l : ls ) {
          ValueMetaBase vmb = (ValueMetaBase) l;
          wMessageField.add( vmb.getName() );
        }
      } catch ( KettleStepException ex ) {
        // do nothing
      }
    };
    wMessageField.getCComboWidget().addListener( SWT.FocusIn, lsMessageFocus );

    FormData fdSetupComp = new FormData();
    fdSetupComp.left = new FormAttachment( 0, 0 );
    fdSetupComp.top = new FormAttachment( 0, 0 );
    fdSetupComp.right = new FormAttachment( 100, 0 );
    fdSetupComp.bottom = new FormAttachment( 100, 0 );
    wSetupComp.setLayoutData( fdSetupComp );
    wSetupComp.layout();
    wSetupTab.setControl( wSetupComp );

  }

  private void exchangeChanged() {

    boolean defaultSelected = "default".equals(wExchangeType.getText());

    wlExchangeName.setEnabled(!defaultSelected);
    wExchangeName.setEnabled(!defaultSelected);
    wExchangeDurable.setEnabled(!defaultSelected);
    wExchangeExclusive.setEnabled(!defaultSelected);
    wExchangeAutoDelete.setEnabled(!defaultSelected);
    wlRoutingKey.setEnabled(!defaultSelected);
    wRoutingKey.setEnabled(!defaultSelected);

    wlQueueName.setEnabled(defaultSelected);
    wQueueName.setEnabled(defaultSelected);
    wQueueDurable.setEnabled(defaultSelected);
    wQueueExclusive.setEnabled(defaultSelected);
    wQueueAutoDelete.setEnabled(defaultSelected);
  }



  protected void getData() {
    wHostname.setText( nullToEmpty( meta.getHostname() ) );

    wExchangeType.setText( nullToEmpty( meta.getExchangeType() ) );
    wExchangeName.setText( nullToEmpty( meta.getExchange() ) );
    wExchangeDurable.setSelection(meta.isExchangeDurable());
    wExchangeExclusive.setSelection( meta.isExchangeExclusive() );
    wExchangeAutoDelete.setSelection( meta.isExchangeAutoDelete() );

    wQueueName.setText( nullToEmpty( meta.getQueue() ) );
    wQueueDurable.setSelection(meta.isQueueDurable());
    wQueueExclusive.setSelection( meta.isQueueExclusive() );
    wQueueAutoDelete.setSelection( meta.isQueueAutoDelete() );

    wRoutingKey.setText( nullToEmpty( meta.getRoutingKey() ) );
    wMessageField.setText( nullToEmpty( meta.getFieldToSend() ) );

    exchangeChanged();
  }

  private void ok() {
    stepname = wStepname.getText();

    meta.setHostname(wHostname.getText());

    meta.setExchangeType(wExchangeType.getText());
    meta.setExchange(wExchangeName.getText());
    meta.setExchangeDurable(wExchangeDurable.getSelection());
    meta.setExchangeExclusive(wExchangeExclusive.getSelection());
    meta.setExchangeAutoDelete(wExchangeAutoDelete.getSelection());

    meta.setQueue(wQueueName.getText());
    meta.setQueueDurable(wQueueDurable.getSelection());
    meta.setQueueExclusive(wQueueExclusive.getSelection());
    meta.setQueueAutoDelete(wQueueAutoDelete.getSelection());

    meta.setRoutingKey(wRoutingKey.getText());
    meta.setFieldToSend(wMessageField.getText());

    dispose();
  }

  private void cancel() {
    meta.setChanged( false );
    dispose();
  }

  private Image getImage() {
    PluginInterface plugin =
            PluginRegistry.getInstance().getPlugin( StepPluginType.class, stepMeta.getStepMetaInterface() );
    String id = plugin.getIds()[ 0 ];
    if ( id != null ) {
      return GUIResource.getInstance().getImagesSteps().get( id ).getAsBitmapForSize( shell.getDisplay(),
              ConstUI.LARGE_ICON_SIZE, ConstUI.LARGE_ICON_SIZE );
    }
    return null;
  }
}
