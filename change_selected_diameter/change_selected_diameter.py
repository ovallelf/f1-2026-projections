#Author-GitHub Copilot
#Description-Change the diameter or radius of selected circular edges, faces, or sketch circles

import adsk.core, adsk.fusion, traceback
import math

def run(context):
    ui = None
    try:
        app = adsk.core.Application.get()
        ui = app.userInterface
        design = adsk.fusion.Design.cast(app.activeProduct)
        
        if not design:
            ui.messageBox('No active Fusion 360 design found.')
            return
        
        # Prompt user for selection
        selection_input = ui.selectEntity(
            'Select a circular edge, cylindrical face, or sketch circle',
            'Edges,Faces,SketchCurves'
        )
        
        if not selection_input:
            return
        
        selected_entity = selection_input.entity
        current_radius = None
        param_to_change = None
        is_diameter = False
        
        # Handle different selection types
        if isinstance(selected_entity, adsk.fusion.BRepEdge):
            # Circular edge selected
            edge = selected_entity
            if edge.geometry.curveType == adsk.core.Curve3DTypes.Circle3DCurveType:
                circle = adsk.core.Circle3D.cast(edge.geometry)
                current_radius = circle.radius
                
                # Try to find the parameter that controls this edge
                param_to_change = find_controlling_parameter(design, edge, current_radius)
            else:
                ui.messageBox('Selected edge is not circular.')
                return
                
        elif isinstance(selected_entity, adsk.fusion.BRepFace):
            # Cylindrical face selected
            face = selected_entity
            if face.geometry.surfaceType == adsk.core.SurfaceTypes.CylinderSurfaceType:
                cylinder = adsk.core.Cylinder.cast(face.geometry)
                current_radius = cylinder.radius
                
                # Try to find the parameter that controls this face
                param_to_change = find_controlling_parameter(design, face, current_radius)
            else:
                ui.messageBox('Selected face is not cylindrical.')
                return
                
        elif isinstance(selected_entity, adsk.fusion.SketchCircle):
            # Sketch circle selected
            sketch_circle = selected_entity
            current_radius = sketch_circle.radius
            
            # Get the dimension constraint if it exists
            sketch = sketch_circle.parentSketch
            for dim in sketch.sketchDimensions:
                if isinstance(dim, adsk.fusion.SketchRadialDimension):
                    if dim.entity == sketch_circle:
                        param_to_change = dim.parameter
                        break
                elif isinstance(dim, adsk.fusion.SketchDiameterDimension):
                    if dim.entity == sketch_circle:
                        param_to_change = dim.parameter
                        is_diameter = True
                        break
        else:
            ui.messageBox('Please select a circular edge, cylindrical face, or sketch circle.')
            return
        
        if current_radius is None:
            ui.messageBox('Could not determine the radius of the selected geometry.')
            return
        
        current_diameter_mm = current_radius * 20  # radius in cm to diameter in mm
        
        # Ask user for new value
        (new_value_str, cancelled) = ui.inputBox(
            f'Current diameter: {current_diameter_mm:.3f} mm\n\n'
            f'Enter new diameter in mm (or prefix with "r" for radius, e.g., "r14.15"):',
            'Change Diameter/Radius',
            str(round(current_diameter_mm, 3))
        )
        
        if cancelled:
            return
        
        # Parse the input
        new_value_str = new_value_str.strip()
        input_is_radius = False
        
        if new_value_str.lower().startswith('r'):
            input_is_radius = True
            new_value_str = new_value_str[1:].strip()
        
        try:
            new_value_mm = float(new_value_str)
        except ValueError:
            ui.messageBox('Invalid number entered.')
            return
        
        # Convert to radius in cm (Fusion internal units)
        if input_is_radius:
            new_radius_cm = new_value_mm / 10
            new_diameter_mm = new_value_mm * 2
        else:
            new_radius_cm = new_value_mm / 20  # diameter mm to radius cm
            new_diameter_mm = new_value_mm
        
        # Apply the change
        if param_to_change:
            # We found a parameter - change it
            if is_diameter:
                param_to_change.value = new_diameter_mm / 10  # diameter in cm
            else:
                param_to_change.value = new_radius_cm
            
            ui.messageBox(
                f"Changed parameter '{param_to_change.name}' to {new_diameter_mm:.3f} mm diameter.",
                "Success"
            )
        elif isinstance(selected_entity, adsk.fusion.SketchCircle):
            # Direct sketch circle manipulation
            sketch_circle = selected_entity
            sketch_circle.radius = new_radius_cm
            
            ui.messageBox(
                f"Changed sketch circle to {new_diameter_mm:.3f} mm diameter.",
                "Success"
            )
        else:
            # Try to find and modify via timeline
            success = try_modify_via_timeline(design, ui, selected_entity, current_radius, new_radius_cm, new_diameter_mm)
            
            if not success:
                ui.messageBox(
                    f"Could not find a parameter to modify.\n\n"
                    f"Tip: Open the sketch or feature that created this geometry "
                    f"and edit the dimension directly, or use Change Parameters (Modify > Change Parameters).",
                    "Manual Edit Required"
                )
            
    except:
        if ui:
            ui.messageBox('Failed:\n{}'.format(traceback.format_exc()))


def find_controlling_parameter(design, entity, radius_cm):
    """Try to find a parameter that matches the given radius value."""
    all_params = design.allParameters
    
    # Look for parameters with matching value (within tolerance)
    tolerance = 0.0001  # cm
    
    for i in range(all_params.count):
        param = all_params.item(i)
        try:
            # Check if parameter matches radius
            if abs(param.value - radius_cm) < tolerance:
                return param
            # Check if parameter matches diameter
            if abs(param.value - radius_cm * 2) < tolerance:
                return param
        except:
            pass
    
    return None


def try_modify_via_timeline(design, ui, entity, current_radius, new_radius_cm, new_diameter_mm):
    """Try to find and modify the feature in the timeline."""
    timeline = design.timeline
    
    # This is a simplified approach - full implementation would trace
    # the entity back through the timeline to find its defining feature
    
    # For now, search parameters for a close match
    all_params = design.userParameters
    
    for i in range(all_params.count):
        param = all_params.item(i)
        if abs(param.value - current_radius) < 0.0001:
            param.value = new_radius_cm
            ui.messageBox(
                f"Changed user parameter '{param.name}' to {new_diameter_mm:.3f} mm diameter.",
                "Success"
            )
            return True
        if abs(param.value - current_radius * 2) < 0.0001:
            param.value = new_radius_cm * 2
            ui.messageBox(
                f"Changed user parameter '{param.name}' to {new_diameter_mm:.3f} mm diameter.",
                "Success"
            )
            return True
    
    return False


def stop(context):
    pass
