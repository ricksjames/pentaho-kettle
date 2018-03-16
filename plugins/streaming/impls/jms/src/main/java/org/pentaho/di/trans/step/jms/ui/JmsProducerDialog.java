/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.step.jms.ui;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.jms.JmsDelegate;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.trans.step.BaseStepDialog;


public class JmsProducerDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = JmsProducerDialog.class;

  private final ModifyListener lsMod;
  private final Shell parent;
  private final JmsDelegate jmsDelegate;
  private final StepMetaInterface meta;
  private CTabFolder wTabFolder;
  private CTabItem wSetupTab;
  private Composite wSetupComp;

  public JmsProducerDialog( Shell parent, StepMetaInterface meta, JmsDelegate jmsDelegate,
                            TransMeta transMeta, String stepname ) {
    super( parent, meta, transMeta, stepname );
    lsMod = e -> meta.setChanged();
    this.meta = meta;
    this.parent = parent;
    this.jmsDelegate = jmsDelegate;
  }

  @Override public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    setShellImage( shell, meta );
    shell.setMinimumSize( 527, 622 );

    changed = meta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "" ) );

    Label wicon = new Label( shell, SWT.RIGHT );
    wicon.setImage( getImage() );
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment( 0, 0 );
    fdlicon.right = new FormAttachment( 100, 0 );
    wicon.setLayoutData( fdlicon );
    props.setLook( wicon );

    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "JmsProducer.Stepname.Label" ) );
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

    Label spacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    props.setLook( spacer );
    FormData fdSpacer = new FormData();
    fdSpacer.height = 2;
    fdSpacer.left = new FormAttachment( 0, 0 );
    fdSpacer.top = new FormAttachment( wStepname, 15 );
    fdSpacer.right = new FormAttachment( 100, 0 );
    fdSpacer.width = 497;
    spacer.setLayoutData( fdSpacer );

    // Start of tabbed display
    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );
    wTabFolder.setUnselectedCloseVisible( true );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    FormData fdCancel = new FormData();
    fdCancel.right = new FormAttachment( 100, 0 );
    fdCancel.bottom = new FormAttachment( 100, 0 );
    wCancel.setLayoutData( fdCancel );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fdOk = new FormData();
    fdOk.right = new FormAttachment( wCancel, -5 );
    fdOk.bottom = new FormAttachment( 100, 0 );
    wOK.setLayoutData( fdOk );

    Label hSpacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    props.setLook( hSpacer );
    FormData fdhSpacer = new FormData();
    fdhSpacer.height = 2;
    fdhSpacer.left = new FormAttachment( 0, 0 );
    fdhSpacer.bottom = new FormAttachment( wCancel, -15 );
    fdhSpacer.right = new FormAttachment( 100, 0 );
    hSpacer.setLayoutData( fdhSpacer );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( spacer, 15 );
    fdTabFolder.bottom = new FormAttachment( hSpacer, -15 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    wTabFolder.setLayoutData( fdTabFolder );

    //Setup Tab
    wSetupTab = new CTabItem( wTabFolder, SWT.NONE );
    wSetupTab.setText( BaseMessages.getString( PKG, "BaseStreamingDialog.SetupTab" ) );

    wSetupComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wSetupComp );
    FormLayout setupLayout = new FormLayout();
    setupLayout.marginHeight = 15;
    setupLayout.marginWidth = 15;
    wSetupComp.setLayout( setupLayout );

    ConnectionForm connectionForm = new ConnectionForm( wSetupComp, props, transMeta, lsMod, jmsDelegate );
    Group group = connectionForm.layoutForm();
    DestinationForm destinationForm = new DestinationForm( parent, group, props, transMeta, lsMod, "", "" );
    destinationForm.layoutForm();

    FormData fdSetupComp = new FormData();
    fdSetupComp.left = new FormAttachment( 0, 0 );
    fdSetupComp.top = new FormAttachment( 0, 0 );
    fdSetupComp.right = new FormAttachment( 100, 0 );
    fdSetupComp.bottom = new FormAttachment( 100, 0 );
    wSetupComp.setLayoutData( fdSetupComp );
    wSetupComp.layout();
    wSetupTab.setControl( wSetupComp );

    return "";
  }

  private Image getImage() {
    PluginInterface plugin =
      PluginRegistry.getInstance().getPlugin( StepPluginType.class, stepMeta.getStepMetaInterface() );
    String id = plugin.getIds()[ 0 ];
    if ( id != null ) {
      return GUIResource.getInstance().getImagesSteps().get( id ).getAsBitmapForSize( shell.getDisplay(),
        ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
    }
    return null;
  }
}
