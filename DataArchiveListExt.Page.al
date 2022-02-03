pageextension 50100 "Data Archive List Ext" extends "Data Archive List"
{
    actions
    {
        addfirst(Processing)
        {
            action(Export2adls)
            {
                Caption = 'Export 2 ADLS';
                ApplicationArea = All;

                trigger OnAction()
                begin
                    StartArchive(Rec);
                end;
            }
        }
    }

    procedure StartArchive(DataArchive: Record "Data Archive")
    var
        DataArchiveTable: Record "Data Archive Table";
        Payload: TextBuilder;
        UpdatedLastTimeStamp: BigInteger;
        FieldIdList: List of [Integer];
        EntityJsonNeedsUpdate: Boolean;
        ManifestJsonsNeedsUpdate: Boolean;
        ADLSE: Codeunit ADLSE;
        TempBlob: Codeunit "Temp Blob";
        OutStr: OutStream;
        RecRef: RecordRef;
        SchemaJson: JsonArray;
    begin
        DataArchiveTable.SetRange("Data Archive Entry No.", DataArchive."Entry No.");
        if DataArchiveTable.FindSet() then
            repeat
                Clear(Payload);
                RecRef.Open(DataArchiveTable."Table No.");

                //Header
                Clear(TempBlob);
                Clear(OutStr);
                TempBlob.CreateOutStream(OutStr, TextEncoding::UTF8);
                DataArchiveTable."Table Fields (json)".ExportStream(OutStr);
                ADLSE.WriteHeadersToCsvStream(TempBlob, Payload, RecRef, SchemaJson);
                //Lines
                Clear(TempBlob);
                Clear(OutStr);
                TempBlob.CreateOutStream(OutStr, TextEncoding::UTF8);
                DataArchiveTable."Table Data (json)".ExportStream(OutStr);
                ADLSE.WriteLinesToCsvStream(TempBlob, Payload, RecRef, SchemaJson);

                ADLSE.AppendPayload(Payload);
                FieldIdList := CreateFieldListForTable(DataArchiveTable."Table No.");
                ADLSE.Init(DataArchiveTable."Table No.", FieldIdList, UpdatedLastTimeStamp, false);
                ADLSE.CreateDataBlob();
                ADLSE.FlushPayload();

                ADLSE.CheckEntity(EntityJsonNeedsUpdate, ManifestJsonsNeedsUpdate);
                ADLSE.TryUpdateCdmJsons(EntityJsonNeedsUpdate, ManifestJsonsNeedsUpdate);

                ADLSE.Flush();

                RecRef.Close;
            //DataArchiveTable.Delete(); //because it is exported
            until DataArchiveTable.Next = 0;
        //rec.Delete(); //because it is exported

        CurrPage.Update();
    end;

    procedure CreateFieldListForTable(TableID: Integer) FieldIdList: List of [Integer]
    var
        Fld: Record Field;
    begin
        Fld.SetRange(TableNo, TableID);
        Fld.SetRange(Class, Fld.Class::Normal);
        Fld.SetFilter(Type, '<>%1', Fld.Type::MediaSet);
        Fld.SetFilter(ObsoleteState, '<>%1', Fld.ObsoleteState::Removed);
        if Fld.FindSet() then
            repeat
                FieldIdList.Add(Fld."No.");
            until Fld.Next = 0;

        FieldIdList.Add(0); // Timestamp field
    end;
}